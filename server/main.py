from __future__ import annotations
from io import BytesIO
import secrets
import traceback
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, File, UploadFile, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook

from wb_io.wb_readers import (
    read_stock_history,
    read_stock_snapshot,
    read_intransit_file,
    read_fulfillment_stock_file,
    read_sku_reference,
)
from engine.transform import (
    sales_by_warehouse_from_details,
    merge_sales_with_stock_today,
    stock_from_snapshot,
)
from engine.export_prototype import build_prototype_workbook
import math

# Название листа с продажами в итоговом Excel (для логов)
SHEET_SALES_NAME = "Продажи по складам"

_SALES_STOCK_COLUMNS = [
    "Артикул продавца",
    "Артикул WB",
    "Склад",
    "Заказали, шт",
    "Остаток на сегодня",
]

app = FastAPI()
templates = Jinja2Templates(directory="server/templates")

try:
    app.mount("/static", StaticFiles(directory="server/static"), name="static")
except Exception:
    pass

_memory_artifacts: Dict[str, bytes] = {}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/download/fulfillment_template.xlsx")
async def download_fulfillment_template():
    """Генерирует и отдает XLSX-шаблон «Остатки Фулфилмент»"""
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Шаблон"
    worksheet.append(["Артикул продавца", "Количество"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="fulfillment_template.xlsx"'
        },
    )

# [WB_ANCHOR] build endpoint
@app.post("/build")
async def build(
    files: List[UploadFile] = File(...),
    dummy: int = Query(0, ge=0, le=1),
):
    logs: List[str] = []
    combined_frames: List[pd.DataFrame] = []
    supplies_frames: List[pd.DataFrame] = []
    fulfillment_frames: List[pd.DataFrame] = []
    daily_frames: List[pd.DataFrame] = []  # История остатков по дням (по сети)
    sku_ref: pd.DataFrame | None = None
    sku_map: Dict[str, Dict[str, str | None]] = {}
    unknown_sku: List[str] = []
    unknown_seen: set[tuple[str, str | None, str | None, str | None]] = set()

    if not files and dummy != 1:
        return JSONResponse({"ok": False, "log": "Файлы не переданы"}, status_code=400)

    try:
        if dummy == 1:
            logs.append("SMOKE: dummy=1 — сборка на встроенных фикстурах")
            daily_frames.append(
                pd.DataFrame(
                    {
                        "Артикул продавца": ["DUMMY_SKU"],
                        "Артикул WB": ["000000"],
                        "01.10.2025": [10],
                        "02.10.2025": [12],
                        "03.10.2025": [0],
                    }
                )
            )
            detail_dummy = pd.DataFrame(
                {
                    "Склад": ["Тула", "Казань"],
                    "Артикул продавца": ["DUMMY_SKU", "DUMMY_SKU"],
                    "Артикул WB": ["000000", "000000"],
                    "Заказали, шт": [8, 4],
                }
            )
            merged = merge_sales_with_stock_today(detail_dummy, detail_dummy, daily_frames[0])
            if not merged.empty:
                combined_frames.append(merged)
            logs.append("SMOKE: добавлены фикстуры — продажи и остатки по дням (1 SKU)")
            sku_ref = pd.DataFrame(
                {"seller_sku": ["DUMMY_SKU"], "wb_sku": ["000000"], "barcode": [None]}
            )
        else:
            for f in files:
                raw = await f.read()

                # 0) Справочник SKU
                sku_candidate: pd.DataFrame | None = None
                try:
                    sku_candidate = read_sku_reference(raw, f.filename)
                except Exception:
                    sku_candidate = None
                if sku_candidate is not None and not sku_candidate.empty:
                    sku_ref = sku_candidate
                    logs.append(
                        f"{f.filename}: источник «Справочник SKU» — {len(sku_ref)} строк"
                    )
                    continue

                # 1) СНАЧАЛА: «Остатки Фулфилмент» (простой файл с двумя колонками)
                fulfillment_one = read_fulfillment_stock_file(raw, f.filename)
                if not fulfillment_one.empty:
                    fulfillment_frames.append(fulfillment_one)
                    logs.append(
                        f"{f.filename}: источник «Остатки Фулфилмент» — {len(fulfillment_one)} строк"
                    )
                    continue

                # 2) ЗАТЕМ: «Поставки в пути»
                intransit_df = read_intransit_file(raw, f.filename)
                if intransit_df.attrs.get("intransit"):
                    supplies_frames.append(intransit_df)
                    logs.append(
                        f"{f.filename}: источник «Поставки в пути» — {len(intransit_df)} строк"
                    )
                    continue

                # 3) ПОТОМ: «Остатки по складам» (снимок)
                snapshot_df = read_stock_snapshot(raw, f.filename)
                if snapshot_df is not None:
                    df_stock = stock_from_snapshot(snapshot_df)
                    if not df_stock.empty:
                        df_stock = df_stock.rename(columns={"Остаток": "Остаток на сегодня"})
                        df_stock["Заказали, шт"] = 0
                        df_stock["Склад"] = df_stock["Склад"].fillna("-").replace("", "-")
                        df_stock = df_stock[_SALES_STOCK_COLUMNS]
                        combined_frames.append(df_stock)
                    logs.append(
                        f"{f.filename}: источник «Остатки по складам» — {len(df_stock)} строк"
                    )
                    continue

                # 4) ИНАЧЕ: «История остатков» (детали + по дням)
                sheets = read_stock_history(raw, f.filename)
                if not sheets:
                    logs.append(f"{f.filename}: источник не распознан")
                    continue

                detail = sheets.get("Детальная информация")
                daily = sheets.get("Остатки по дням")

                df_sales = (
                    sales_by_warehouse_from_details(detail)
                    if detail is not None
                    else pd.DataFrame()
                )
                merged = merge_sales_with_stock_today(df_sales, detail, daily)
                if not merged.empty:
                    combined_frames.append(merged)

                logs.append(
                    f"{f.filename}: источник «История остатков» — {len(merged)} строк"
                )
                # Сбор дневных остатков (по сети) для вкладки «История остатков по дням»
                if daily is not None and not daily.empty:
                    df = daily.copy()
                    id_cols = ["Артикул продавца", "Артикул WB"]
                    date_cols = [c for c in df.columns if c not in id_cols]
                    if date_cols:
                        for c in date_cols:
                            df[c] = pd.to_numeric(
                                df[c]
                                .astype(str)
                                .str.replace("\xa0", "", regex=False)
                                .str.replace(" ", "", regex=False)
                                .str.replace(",", ".", regex=False),
                                errors="coerce",
                            ).fillna(0)
                        daily_frames.append(df[id_cols + date_cols])

        # ---- SKU VALIDATION ---------------------------------------------------
        if sku_ref is None or sku_ref.empty:
            logs.append("❌ Справочник SKU не найден. Невозможно выполнить валидацию.")
            return {"ok": False, "log": "\n".join(logs)}

        for _, ref_row in sku_ref.iterrows():
            seller_value = ref_row.get("seller_sku")
            wb_value = ref_row.get("wb_sku")
            barcode_value = ref_row.get("barcode")

            def _normalize_ref(value: object) -> str | None:
                if pd.isna(value):
                    return None
                text = str(value).strip()
                return text or None

            seller = _normalize_ref(seller_value)
            wb = _normalize_ref(wb_value)
            barcode = _normalize_ref(barcode_value)
            if not (seller or wb or barcode):
                continue
            record = {"seller": seller, "wb": wb, "barcode": barcode}
            for key in (seller, wb, barcode):
                if key:
                    sku_map[key] = record

        def _pick_value(row: pd.Series, names: tuple[str, ...]) -> str | None:
            for name in names:
                if name in row:
                    value = row[name]
                    if pd.isna(value):
                        continue
                    text = str(value).strip()
                    if text:
                        return text
            return None

        seller_columns = (
            "Артикул продавца",
            "Артикул",
            "Артикул поставщика",
        )
        wb_columns = (
            "Артикул WB",
            "Артикул ВБ",
            "Артикул Wildberries",
        )
        barcode_columns = (
            "Штрихкод",
            "Штрих-код",
            "Штрих код",
            "barcode",
            "Barcode",
            "Баркод",
        )

        def validate_sku(seller: str | None, wb: str | None, barcode: str | None, origin: str) -> None:
            key_candidates = [seller, wb, barcode]
            found = None
            for candidate in key_candidates:
                if candidate and candidate in sku_map:
                    found = sku_map[candidate]
                    break
            if not found:
                marker = (origin, seller, wb, barcode)
                if marker not in unknown_seen:
                    unknown_seen.add(marker)
                    unknown_sku.append(
                        f"{origin}: seller='{seller or ''}', wb='{wb or ''}', barcode='{barcode or ''}'"
                    )

        validation_sources: List[tuple[pd.DataFrame, str]] = []
        validation_sources.extend((df, "Продажи и остатки") for df in combined_frames)
        validation_sources.extend((df, "Остатки Фулфилмент") for df in fulfillment_frames)
        validation_sources.extend((df, "Поставки в пути") for df in supplies_frames)

        for df_check, origin in validation_sources:
            if df_check is None or df_check.empty:
                continue
            for row_idx, (_, row) in enumerate(df_check.iterrows(), start=1):
                seller = _pick_value(row, seller_columns)
                wb = _pick_value(row, wb_columns)
                barcode = _pick_value(row, barcode_columns)
                if not (seller or wb or barcode):
                    continue
                validate_sku(seller, wb, barcode, f"{origin}, строка {row_idx}")

        if unknown_sku:
            logs.append("❌ Не удалось валидировать SKU:")
            logs.extend(unknown_sku)
            return {"ok": False, "log": "\n".join(logs)}

        logs.append(f"✓ Все SKU валидированы: {len(sku_map)} записей в справочнике.")

        if combined_frames:
            sales_stock = pd.concat(combined_frames, ignore_index=True).reindex(columns=_SALES_STOCK_COLUMNS)
        else:
            sales_stock = pd.DataFrame(columns=_SALES_STOCK_COLUMNS)

        logs.append(f"Итог: «{SHEET_SALES_NAME}» — {len(sales_stock)}.")

        # Свести «Историю остатков по дням» из всех загруженных файлов
        daily_stock_df = None
        if daily_frames:
            base = daily_frames[0]
            for extra in daily_frames[1:]:
                base = base.merge(
                    extra,
                    on=["Артикул продавца", "Артикул WB"],
                    how="outer",
                )
            id_cols = ["Артикул продавца", "Артикул WB"]
            non_id = [c for c in base.columns if c not in id_cols]
            if non_id:
                daily_stock_df = (
                    base.groupby(id_cols, dropna=False, as_index=False)[non_id].max()
                )
            else:
                daily_stock_df = base[id_cols].drop_duplicates().reset_index(drop=True)
            logs.append(
                f"Итог: «История остатков по дням» — {len(daily_stock_df)} SKU"
            )

        supplies_df = pd.concat(supplies_frames, ignore_index=True) if supplies_frames else None
        fulfillment_df = (
            pd.concat(fulfillment_frames, ignore_index=True)
            if fulfillment_frames
            else None
        )
        bio: BytesIO = build_prototype_workbook(
            sales_stock,
            supplies_df=supplies_df,
            fulfillment_df=fulfillment_df,
            daily_stock_df=daily_stock_df,
        )
        token = secrets.token_urlsafe(16)
        _memory_artifacts[token] = bio.getvalue()

        if supplies_df is not None:
            logs.append(f"Итог: «Поставки в пути» — {len(supplies_df)} строк")
        if fulfillment_df is not None:
            logs.append(
                f"Итог: «Остатки Фулфилмент» — {len(fulfillment_df)} строк"
            )
        return {"ok": True, "log": "\n".join(logs), "download_token": token}

    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse(
            {"ok": False, "log": "\n".join(logs + [f'Ошибка: {e}', "TRACEBACK:", tb])},
            status_code=500,
        )

@app.get("/download/{token}")
async def download(token: str):
    blob = _memory_artifacts.pop(token, None)
    if not blob:
        return JSONResponse({"ok": False, "log": "Файл не найден или срок истёк"}, status_code=404)
    # Если формат старый (просто bytes) — используем имя по умолчанию.
    if isinstance(blob, bytes):
        data = blob
        filename = "Input_Prototype_Filled.xlsx"
    else:
        data = blob.get("data")
        filename = blob.get("filename", "result.xlsx")

    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# --------------------------- Рекомендации ---------------------------
@app.post("/recommend")
async def recommend(files: List[UploadFile] = File(...)):
    logs: List[str] = []
    if not files:
        return JSONResponse({"ok": False, "log": "Файл не передан"}, status_code=400)
    try:
        raw = await files[0].read()
        bio = BytesIO(raw)
        xls = pd.ExcelFile(bio)
        # нужен лист «Продажи по складам»
        sheet_name = None
        for name in xls.sheet_names:
            if str(name).strip().lower() == "продажи по складам":
                sheet_name = name
                break
        if sheet_name is None:
            return JSONResponse({"ok": False, "log": "Не найден лист «Продажи по складам» в загруженном файле"}, status_code=400)
        df = xls.parse(sheet_name)
        # нормализуем шапку
        cols = {str(c).strip().lower(): c for c in df.columns}

        def pick(cands):
            for k in cands:
                if k in cols:
                    return cols[k]
            return None

        c_seller = pick(["артикул продавца"])
        c_wb = pick(["артикул wb", "артикул wb."])
        c_wh = pick(["склад"])
        c_avg = pick(["средние продажи в день", "средние продажи/день", "средние продажи"])
        needed = [c_seller, c_wb, c_wh, c_avg]
        if any(c is None for c in needed):
            return JSONResponse(
                {"ok": False, "log": "В листе «Продажи по складам» отсутствуют нужные колонки"},
                status_code=400,
            )
        df_base = pd.DataFrame(
            {
                "Артикул продавца": df[c_seller],
                "Артикул WB": df[c_wb],
                "Склад": df[c_wh],
                "Средние продажи в день": pd.to_numeric(df[c_avg], errors="coerce").fillna(0.0),
            }
        )
        c_stock = pick(["остаток на сегодня", "остаток"])
        if c_stock is not None:
            df_base["Остаток на сегодня"] = (
                pd.to_numeric(df[c_stock], errors="coerce").fillna(0.0)
            )
        else:
            df_base["Остаток на сегодня"] = 0.0

        # Берём MinStock и MOQ из соответствующих листов
        # --- MinStock ---
        minstock_sheet = None
        for name in xls.sheet_names:
            if str(name).strip().lower() == "minstock":
                minstock_sheet = name
                break
        minstock_map: Dict[tuple[str, str], float] = {}
        if minstock_sheet:
            ms = xls.parse(minstock_sheet)
            cols_ms = {str(c).strip().lower(): c for c in ms.columns}
            c_ms_seller = cols_ms.get("артикул продавца")
            c_ms_wb = cols_ms.get("артикул wb")
            c_ms_val = cols_ms.get("значение")
            if c_ms_seller and c_ms_wb and c_ms_val:
                for _, row in ms.iterrows():
                    key = (str(row[c_ms_seller]).strip(), str(row[c_ms_wb]).strip())
                    try:
                        minstock_map[key] = float(row[c_ms_val])
                    except Exception:
                        continue

        # --- MOQ ---
        moq_sheet = None
        for name in xls.sheet_names:
            if str(name).strip().lower() == "moq":
                moq_sheet = name
                break
        moq_map: Dict[tuple[str, str], float] = {}
        if moq_sheet:
            mo = xls.parse(moq_sheet)
            cols_mo = {str(c).strip().lower(): c for c in mo.columns}
            c_mo_seller = cols_mo.get("артикул продавца")
            c_mo_wb = cols_mo.get("артикул wb")
            c_mo_val = cols_mo.get("moq")
            if c_mo_seller and c_mo_wb and c_mo_val:
                for _, row in mo.iterrows():
                    key = (str(row[c_mo_seller]).strip(), str(row[c_mo_wb]).strip())
                    try:
                        moq_map[key] = float(row[c_mo_val])
                    except Exception:
                        continue

        # --- Окна приёмки ---
        acceptance_days = 0
        acc_sheet = None
        for name in xls.sheet_names:
            if str(name).strip().lower() == "окна приёмки":
                acc_sheet = name
                break
        if acc_sheet:
            ac = xls.parse(acc_sheet)
            cols_ac = {str(c).strip().lower(): c for c in ac.columns}
            c_days = cols_ac.get("количество дней")
            if c_days is not None and not ac.empty:
                try:
                    acceptance_days = int(ac[c_days].iloc[0])
                except Exception:
                    acceptance_days = 0

        # --- Фильтр складов + частота подсортировок ---
        selected_wh: List[str] | None = None
        freq_map: Dict[str, int] = {}
        wh_sheet = None
        for name in xls.sheet_names:
            if str(name).strip().lower() == "склады для подсортировки":
                wh_sheet = name
                break
        if wh_sheet:
            wh = xls.parse(wh_sheet)
            cols_wh = {str(c).strip().lower(): c for c in wh.columns}
            c_wh_name = cols_wh.get("склад")
            c_sel = cols_wh.get("выбрать")
            c_freq = cols_wh.get("частота подсортировок, дни")

            def _is_selected(v: object) -> bool:
                return str(v).strip().lower() in ("1", "true", "да", "истина", "yes")

            if c_wh_name and c_sel:
                selected_wh = []
                for _, row in wh.iterrows():
                    wh_name = str(row[c_wh_name]).strip()
                    if not wh_name:
                        continue
                    if _is_selected(row[c_sel]):
                        selected_wh.append(wh_name)
                    if c_freq:
                        try:
                            freq_map[wh_name] = int(row[c_freq])
                        except Exception:
                            freq_map[wh_name] = freq_map.get(wh_name, 0)
                selected_wh = list(dict.fromkeys(selected_wh))

        if selected_wh:
            logs.append(
                f"Фильтр складов: выбрано {len(selected_wh)} — {', '.join(selected_wh)}"
            )
        else:
            logs.append("Фильтр складов: не задан (используются все склады)")

        df_base["Склад"] = df_base["Склад"].astype(str)

        results: Dict[str, pd.DataFrame] = {}

        for wh_name in (selected_wh if selected_wh else [None]):
            if wh_name is None:
                subset = df_base.copy()
            else:
                subset = df_base[df_base["Склад"] == wh_name].copy()
            if subset.empty:
                continue

            rec_rows: List[Dict[str, object]] = []
            for _, row in subset.iterrows():
                seller = str(row["Артикул продавца"]).strip()
                wb = str(row["Артикул WB"]).strip()
                key = (seller, wb)

                avg = float(row["Средние продажи в день"])
                stock_now = float(row["Остаток на сегодня"])

                minstock = minstock_map.get(key, 0.0)
                moq = moq_map.get(key, 0.0)
                freq = freq_map.get(wh_name or "", 0)
                horizon = acceptance_days + freq

                target = minstock + avg * horizon
                order_raw = target - stock_now
                order = max(0.0, order_raw)
                if moq > 0:
                    order = math.ceil(order / moq) * moq

                rec_rows.append(
                    {
                        "Артикул продавца": seller,
                        "Артикул WB": wb,
                        "Склад": wh_name if wh_name is not None else str(row["Склад"]),
                        "Средние продажи в день": avg,
                        "MinStock": minstock,
                        "Горизонт, дни": horizon,
                        "MOQ": moq,
                        "Остаток на сегодня": stock_now,
                        "Рекомендация, шт": int(order),
                    }
                )

            sheet_key = wh_name or "Рекомендации"
            results[sheet_key] = pd.DataFrame(rec_rows)

        if not results:
            results["Рекомендации"] = pd.DataFrame(
                columns=[
                    "Артикул продавца",
                    "Артикул WB",
                    "Склад",
                    "Средние продажи в день",
                    "MinStock",
                    "Горизонт, дни",
                    "MOQ",
                    "Остаток на сегодня",
                    "Рекомендация, шт",
                ]
            )

        out = BytesIO()
        try:
            writer = pd.ExcelWriter(out, engine="xlsxwriter")
        except Exception:
            writer = pd.ExcelWriter(out, engine="openpyxl")

        def _clean(name: str) -> str:
            raw = str(name)
            bad = ":*?/\\[]"
            cleaned = "".join(ch for ch in raw if ch not in bad).strip()
            return cleaned[:31] or "Склад"

        def _autofit_sheet(writer, sheet_name, df):
            if df is None or df.empty:
                return
            widths = []
            for col in df.columns:
                try:
                    maxlen = max(df[col].astype(str).map(len).max(), len(str(col)))
                except Exception:
                    maxlen = len(str(col))
                widths.append(min(maxlen + 2, 60))
            try:
                ws = writer.sheets.get(sheet_name)
                if ws is not None and hasattr(ws, "set_column"):
                    for idx, w in enumerate(widths):
                        ws.set_column(idx, idx, max(8, w))
                    return
            except Exception:
                pass
            try:
                from openpyxl.utils import get_column_letter
                ws = writer.book[sheet_name]
                for idx, w in enumerate(widths, start=1):
                    ws.column_dimensions[get_column_letter(idx)].width = max(8, w)
            except Exception:
                pass

        with writer:
            for wh_name, df_wh in results.items():
                sheet_name = _clean(wh_name)
                df_wh.to_excel(writer, sheet_name=sheet_name, index=False)
                _autofit_sheet(writer, sheet_name, df_wh)
        out.seek(0)
        token = secrets.token_urlsafe(16)
        _memory_artifacts[token] = {
            "data": out.getvalue(),
            "filename": "WB_Replenishment_Recommendations.xlsx"
        }
        logs.append(f"Рекомендации сформированы: {len(df_base)} строк")
        return {"ok": True, "log": "\n".join(logs), "download_token": token}
    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse({"ok": False, "log": "\n".join(logs + [f'Ошибка: {e}', 'TRACEBACK:', tb])}, status_code=500)
