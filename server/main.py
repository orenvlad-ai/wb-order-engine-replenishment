from __future__ import annotations
from io import BytesIO
import secrets
import traceback
from typing import Dict, List
import asyncio

import pandas as pd
from fastapi import FastAPI, File, UploadFile, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
import math

from wb_io.wb_readers import (
    read_stock_history,
    read_stock_snapshot,
    read_intransit_file,
    read_fulfillment_stock_file,
)
from engine.transform import (
    sales_by_warehouse_from_details,
    merge_sales_with_stock_today,
    stock_from_snapshot,
)
from engine.export_prototype import build_prototype_workbook

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

# ------------------------------ FF helpers (recommend) ------------------------------
def _ff_norm_id(v):
    import math
    if v is None: return None
    if isinstance(v, float) and math.isnan(v): return None
    s = str(v).strip().replace("\xa0", "")
    s = s.replace(" ", "")
    if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
    return s or None

def _ff_read_sheet(xl, name: str) -> pd.DataFrame:
    try:
        df = xl.parse(name)
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()

def _ff_build_index_and_filter(book_bytes: bytes):
    """
    Возвращает:
      ff_by_pair: {(seller, wb) -> qty}, ff_by_seller: {seller -> qty}, ff_by_wb: {wb -> qty},
      selected_wh: множество выбранных складов,
      diag: список строк‑диагностики для логов.
    """
    from io import BytesIO
    diag = []
    ff_by_pair, ff_by_seller, ff_by_wb = {}, {}, {}
    selected_wh: set[str] = set()
    if not book_bytes:
        diag.append("FF: входная книга не прочитана (bytes=0) — квотирование пропущено.")
        return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh, diag
    try:
        xl = pd.ExcelFile(BytesIO(book_bytes))
    except Exception:
        diag.append("FF: не удалось открыть входную книгу — квотирование пропущено.")
        return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh, diag
    # Остатки ФФ
    ff = _ff_read_sheet(xl, "Остатки Фулфилмент")
    if not ff.empty:
        cols = {str(c).strip().lower(): c for c in ff.columns}
        s_col = cols.get("артикул продавца") or cols.get("артикул поставщика") or cols.get("артикул")
        w_col = cols.get("артикул wb") or cols.get("артикул wildberries") or cols.get("артикул вб") or cols.get("код товара")
        q_col = cols.get("количество") or cols.get("остаток") or cols.get("кол-во") or cols.get("шт")
        if q_col:
            tmp = ff.copy()
            if s_col: tmp[s_col] = tmp[s_col].map(_ff_norm_id)
            if w_col: tmp[w_col] = tmp[w_col].map(_ff_norm_id)
            tmp[q_col] = pd.to_numeric(tmp[q_col], errors="coerce").fillna(0).clip(lower=0)
            total_rows = len(tmp)
            wb_filled = int(tmp[w_col].notna().sum()) if w_col else 0
            diag.append(f"FF: строк={total_rows}, WB заполнено={wb_filled}")
            if s_col and w_col:
                grp = tmp.groupby([s_col, w_col], dropna=False)[q_col].sum()
                for (s,w), v in grp.items(): ff_by_pair[(s,w)] = float(v)
            if s_col:
                grp = tmp.groupby([s_col], dropna=False)[q_col].sum()
                for s, v in grp.items(): ff_by_seller[s] = float(v)
            if w_col:
                grp = tmp.groupby([w_col], dropna=False)[q_col].sum()
                for w, v in grp.items(): ff_by_wb[w] = float(v)
    else:
        diag.append("FF: лист «Остатки Фулфилмент» пуст — квотирование пропущено.")
    # Склады для подсортировки
    flt = _ff_read_sheet(xl, "Склады для подсортировки")
    if not flt.empty:
        cols = {str(c).strip().lower(): c for c in flt.columns}
        wh_col = cols.get("склад")
        pick_col = cols.get("выбрать")
        if wh_col and pick_col:
            selected_wh = set(
                flt.loc[pd.to_numeric(flt[pick_col], errors="coerce").fillna(0).astype(int) == 1, wh_col].astype(str)
            )
            diag.append(f"Фильтр складов: выбрано {len(selected_wh)} — " + ", ".join(sorted(selected_wh))[:200])
    return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh, diag

def _ff_apply_quota_inplace(df: pd.DataFrame, files: List[UploadFile], logs: List[str]) -> None:
    """
    Обновляет df IN‑PLACE: добавляет/пересчитывает колонку «Рекомендация с учётом остатков FF».
    Сумма по SKU среди выбранных складов не превышает FF.
    """
    base_col = "Рекомендация, шт"
    if df is None or df.empty or base_col not in df.columns:
        logs.append("FF: не найден DataFrame с колонкой «Рекомендация, шт» — пропуск.")
        return
    need_cols = ("Артикул продавца", "Артикул WB", "Склад")
    for c in need_cols:
        if c not in df.columns:
            df["Рекомендация с учётом остатков FF"] = pd.to_numeric(df[base_col], errors="coerce").fillna(0).astype(int)
            logs.append(f"FF: отсутствует колонка «{c}» в рекомендациях — FF=базовой.")
            return
    # Читаем входную книгу надёжно
    book_bytes: bytes = b""
    try:
        f0 = files[0]
        try:
            pos = f0.file.tell()
            f0.file.seek(0)
            book_bytes = f0.file.read()
            f0.file.seek(pos)
        except Exception:
            book_bytes = asyncio.get_event_loop().run_until_complete(f0.read()) if hasattr(asyncio, "get_event_loop") else b""
            try:
                f0.file.seek(0)
            except Exception:
                pass
    except Exception:
        book_bytes = b""
    ff_by_pair, ff_by_seller, ff_by_wb, selected_wh, diag = _ff_build_index_and_filter(book_bytes)
    logs.extend(diag)
    # Гарантируем наличие столбца
    if "Рекомендация с учётом остатков FF" not in df.columns:
        df["Рекомендация с учётом остатков FF"] = pd.to_numeric(df[base_col], errors="coerce").fillna(0).astype(int)
    limited_cnt = 0
    matched_pair = matched_seller = matched_wb = 0
    for (s_raw, w_raw), grp in df.groupby(["Артикул продавца", "Артикул WB"], dropna=False):
        s = _ff_norm_id(s_raw); w = _ff_norm_id(w_raw)
        ff_total = ff_by_pair.get((s, w))
        if ff_total is not None: matched_pair += 1
        if ff_total is None and w is not None:
            ff_total = ff_by_wb.get(w); matched_wb += int(ff_total is not None)
        if ff_total is None and s is not None:
            ff_total = ff_by_seller.get(s); matched_seller += int(ff_total is not None)
        idx = grp.index
        sel = idx if not selected_wh else grp.loc[grp["Склад"].astype(str).isin(selected_wh)].index
        base = pd.to_numeric(df.loc[sel, base_col], errors="coerce").fillna(0).clip(lower=0)
        if ff_total is None or float(base.sum()) <= float(ff_total):
            df.loc[sel, "Рекомендация с учётом остатков FF"] = base.astype(int)
            continue
        # квоты по Коэф. склада
        if "Коэф. склада" in df.columns:
            coefs = pd.to_numeric(df.loc[sel, "Коэф. склада"], errors="coerce").fillna(0).clip(lower=0)
        else:
            coefs = pd.Series(1.0, index=sel, dtype="float64")
        sum_coef = float(coefs[coefs > 0].sum())
        if sum_coef <= 0:
            denom = float(base.sum()) or 1.0
            quota = float(ff_total) * (base / denom)
        else:
            quota = float(ff_total) * (coefs / sum_coef)
        limited = pd.concat([base, quota], axis=1).min(axis=1).clip(lower=0).astype(int)
        df.loc[sel, "Рекомендация с учётом остатков FF"] = limited
        limited_cnt += 1
    logs.append(f"FF: ограничены SKU={limited_cnt}, match pair={matched_pair}, seller={matched_seller}, wb={matched_wb}")

_memory_artifacts: Dict[str, bytes] = {}

# ----------------------------- helpers (recommend) -----------------------------
# Нормализация заголовков и безопасное чтение листа как DataFrame.
def _read_sheet(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    try:
        df = xl.parse(sheet)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


# Квотирование рекомендаций по доступному FF‑остатку.
def _apply_ff_quota(result_df: pd.DataFrame, book_bytes: bytes) -> pd.DataFrame:
    """
    Добавляет колонку 'Рекомендация с учётом остатков FF' на основе:
      – 'Рекомендация, шт' (уже посчитана базовой логикой),
      – листа 'Остатки Фулфилмент' (Артикул продавца | Артикул WB | Количество),
      – и фильтра складов 'Склады для подсортировки' (Выбрать = 1).
    Алгоритм:
      1) агрегируем FF по (seller, wb);
      2) берём строки result_df только по выбранным складам;
      3) если суммарная теоретическая рекомендация <= FF, оставляем как есть;
      4) иначе распределяем FF пропорционально 'Коэф. склада' (только > 0),
         по формуле quota = FF * coef / sum_coef; затем min(theor, quota).
      5) целочисленное округление вниз (int).
    """
    base_col = "Рекомендация, шт"
    if result_df is None or base_col not in result_df.columns:
        return result_df

    try:
        xl = pd.ExcelFile(BytesIO(book_bytes))
    except Exception:
        out = result_df.copy()
        out["Рекомендация с учётом остатков FF"] = pd.to_numeric(out[base_col], errors="coerce").fillna(0).astype(int)
        return out

    ff_df = _read_sheet(xl, "Остатки Фулфилмент")
    filt_df = _read_sheet(xl, "Склады для подсортировки")

    ff_by_wb = pd.Series(dtype="float64")
    ff_by_seller = pd.Series(dtype="float64")
    if not ff_df.empty:
        cols = {str(c).strip().lower(): c for c in ff_df.columns}
        s_col = cols.get("артикул продавца")
        w_col = cols.get("артикул wb") or cols.get("артикул вб") or cols.get("артикул wildberries")
        q_col = cols.get("количество") or cols.get("остаток") or cols.get("кол-во") or cols.get("шт")
        if q_col:
            tmp = ff_df.copy()
            for c in (s_col, w_col):
                if c and c in tmp.columns:
                    tmp[c] = tmp[c].astype(str).str.strip()
            tmp[q_col] = pd.to_numeric(tmp[q_col], errors="coerce").fillna(0)
            if w_col and w_col in tmp.columns:
                ff_by_wb = tmp.groupby(w_col, dropna=False)[q_col].sum()
            if s_col and s_col in tmp.columns:
                ff_by_seller = tmp.groupby(s_col, dropna=False)[q_col].sum()

    selected_wh: set[str] = set()
    if not filt_df.empty:
        cols = {str(c).strip().lower(): c for c in filt_df.columns}
        wh_col = cols.get("склад")
        pick_col = cols.get("выбрать")
        if wh_col and pick_col:
            selected_wh = set(
                filt_df.loc[pd.to_numeric(filt_df[pick_col], errors="coerce").fillna(0).astype(int) == 1, wh_col]
                .astype(str)
            )
    if not selected_wh:
        if "Склад" in result_df.columns:
            selected_wh = set(result_df["Склад"].astype(str))

    out = result_df.copy()
    out["Рекомендация с учётом остатков FF"] = pd.to_numeric(out[base_col], errors="coerce").fillna(0).astype(float)

    id_s = "Артикул продавца"
    id_w = "Артикул WB"
    wh_col = "Склад"
    coef_col = "Коэф. склада"

    if id_s in out.columns and id_w in out.columns and wh_col in out.columns:
        for (seller, wb), grp in out.groupby([id_s, id_w], dropna=False):
            idx_all = grp.index
            idx_sel = grp.loc[grp[wh_col].astype(str).isin(selected_wh)].index
            if len(idx_sel) == 0:
                out.loc[idx_all, "Рекомендация с учётом остатков FF"] = \
                    pd.to_numeric(out.loc[idx_all, base_col], errors="coerce").fillna(0).astype(int)
                continue
            theor = pd.to_numeric(out.loc[idx_sel, base_col], errors="coerce").fillna(0).clip(lower=0)
            total_theor = float(theor.sum())
            seller_key = None if pd.isna(seller) else str(seller).strip()
            wb_key = None if pd.isna(wb) else str(wb).strip()
            ff_total = float("nan")
            if wb_key is not None and not ff_by_wb.empty:
                try:
                    ff_total = float(ff_by_wb.get(wb_key))
                except Exception:
                    ff_total = float("nan")
            if (not ff_by_seller.empty) and (math.isnan(ff_total)) and (seller_key is not None):
                try:
                    ff_total = float(ff_by_seller.get(seller_key))
                except Exception:
                    ff_total = float("nan")
            if math.isnan(ff_total):
                out.loc[idx_sel, "Рекомендация с учётом остатков FF"] = theor.astype(int)
                others = idx_all.difference(idx_sel)
                if len(others):
                    out.loc[others, "Рекомендация с учётом остатков FF"] = \
                        pd.to_numeric(out.loc[others, base_col], errors="coerce").fillna(0).astype(int)
                continue

            if total_theor <= ff_total:
                out.loc[idx_sel, "Рекомендация с учётом остатков FF"] = theor.astype(int)
            else:
                if coef_col in out.columns:
                    coefs = pd.to_numeric(out.loc[idx_sel, coef_col], errors="coerce").fillna(0).clip(lower=0)
                else:
                    coefs = pd.Series([1.0] * len(idx_sel), index=idx_sel, dtype="float64")
                sum_coef = float(coefs[coefs > 0].sum())
                if sum_coef <= 0:
                    denom = total_theor if total_theor > 0 else 1.0
                    quotas = ff_total * (theor / denom)
                else:
                    quotas = ff_total * (coefs / sum_coef)
                limited = pd.concat([theor, quotas], axis=1).min(axis=1)
                out.loc[idx_sel, "Рекомендация с учётом остатков FF"] = limited.fillna(0).astype(int)

                others = idx_all.difference(idx_sel)
                if len(others):
                    out.loc[others, "Рекомендация с учётом остатков FF"] = \
                        pd.to_numeric(out.loc[others, base_col], errors="coerce").fillna(0).astype(int)
    else:
        out["Рекомендация с учётом остатков FF"] = out[base_col].fillna(0).astype(int)
    return out


def _apply_ff_quota_inplace(df: pd.DataFrame, book_bytes: bytes) -> None:
    """
    In-place обновление: рассчитывает колонку «Рекомендация с учётом остатков FF»
    и записывает её прямо в переданный DataFrame (без копий и присваивания через locals()).
    """
    updated = _apply_ff_quota(df, book_bytes)
    if isinstance(updated, pd.DataFrame) and "Рекомендация с учётом остатков FF" in updated.columns:
        # переносим рассчитанный столбец в исходный df
        df["Рекомендация с учётом остатков FF"] = updated["Рекомендация с учётом остатков FF"]

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
        else:
            for f in files:
                raw = await f.read()

                # 0) Справочник SKU — удалено: справочник отключён

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

        # (удалено) SKU‑валидация отключена: сборка не зависит от справочника.
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
        raw: bytes = b""
        f0 = files[0]
        try:
            pos = f0.file.tell()
            f0.file.seek(0)
            raw = f0.file.read()
            f0.file.seek(pos)
        except Exception:
            raw = await f0.read()
            try:
                f0.file.seek(0)
            except Exception:
                pass
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
            results[sheet_key]["Рекомендация, шт"] = (
                pd.to_numeric(results[sheet_key]["Рекомендация, шт"], errors="coerce")
                .fillna(0)
                .astype(int)
            )
            # FF‑квотирование: добавляем/пересчитываем «Рекомендация с учётом остатков FF» в том же DataFrame
            try:
                _ff_apply_quota_inplace(results[sheet_key], files, logs)
            except Exception as _e:
                logs.append(f"FF: ошибка квотирования — {type(_e).__name__}")

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
            try:
                _ff_apply_quota_inplace(results["Рекомендации"], files, logs)
            except Exception as _e:
                logs.append(f"FF: ошибка квотирования — {type(_e).__name__}")

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
        result_df = df_base.copy()
        logs.append(f"Рекомендации сформированы: {len(result_df)} строк")

        # ---------------- FF v6: групповой лимит по SKU, сумма ≤ FF (IN-PLACE в result_df) ----------------
        # 0) Быстрый выход, если базовой колонки нет
        _ff_base_col = "Рекомендация, шт"
        _ff_col = "Рекомендация с учётом остатков FF"
        if _ff_base_col not in result_df.columns:
            logs.append("FF: базовая колонка «Рекомендация, шт» не найдена — квотирование пропущено.")
        else:
            # 1) Читаем байты входной книги (seek→read→seek back) для чтения листов
            _book_bytes: bytes = b""
            try:
                f0 = files[0]
                try:
                    _pos = f0.file.tell()
                    f0.file.seek(0)
                    _book_bytes = f0.file.read()
                    f0.file.seek(_pos)
                except Exception:
                    _book_bytes = await f0.read()
                    try:
                        f0.file.seek(0)
                    except Exception:
                        pass
            except Exception:
                _book_bytes = b""

            # 2) Локальные утилиты (без внешних зависимостей)
            def _ff_norm_header(s: str) -> str:
                t = str(s).strip().lower()
                t = t.replace("\xa0", " ").replace(" ", "")
                t = t.replace(",", "").replace(".", "").replace("ё", "е")
                return t
            def _ff_norm_id(v):
                import math
                if v is None: return None
                if isinstance(v, float) and math.isnan(v): return None
                s = str(v).strip().replace("\xa0", "").replace(" ", "")
                if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
                return s or None

            # 3) Строим карты FF и фильтр выбранных складов
            from io import BytesIO
            _ff_by_wb: dict[str, float] = {}
            _ff_by_seller: dict[str, float] = {}
            _selected_wh: set[str] = set()
            try:
                if _book_bytes:
                    _xl = pd.ExcelFile(BytesIO(_book_bytes))
                    # Остатки Фулфилмент
                    try:
                        _ff = _xl.parse("Остатки Фулфилмент")
                        _ff = _ff.copy(); _ff.columns = [str(c).strip() for c in _ff.columns]
                        _c = { _ff_norm_header(c): c for c in _ff.columns }
                        _s_col = _c.get("артикулпродавца") or _c.get("артикулпоставщика") or _c.get("артикул")
                        _w_col = _c.get("артикулwb") or _c.get("артикулвб") or _c.get("артикулwildberries") or _c.get("кодтовара")
                        _q_col = _c.get("количество") or _c.get("остаток") or _c.get("колво") or _c.get("шт")
                        if _q_col:
                            if _s_col in _ff.columns: _ff[_s_col] = _ff[_s_col].map(_ff_norm_id)
                            if _w_col in _ff.columns: _ff[_w_col] = _ff[_w_col].map(_ff_norm_id)
                            _ff[_q_col] = pd.to_numeric(_ff[_q_col], errors="coerce").fillna(0).clip(lower=0)
                            if _w_col:
                                for _w, _v in _ff.groupby([_w_col], dropna=False)[_q_col].sum().items():
                                    _wk = _ff_norm_id(_w)
                                    if _wk: _ff_by_wb[_wk] = float(_v)
                            if _s_col:
                                for _s, _v in _ff.groupby([_s_col], dropna=False)[_q_col].sum().items():
                                    _sk = _ff_norm_id(_s)
                                    if _sk: _ff_by_seller[_sk] = float(_v)
                            logs.append(f"FF: строк={len(_ff)}, WB заполнено={int(_ff[_w_col].notna().sum()) if _w_col else 0}")
                    except Exception:
                        logs.append("FF: лист «Остатки Фулфилмент» не прочитан — квотирование может быть пропущено.")
                    # Склады для подсортировки
                    try:
                        _flt = _xl.parse("Склады для подсортировки")
                        _flt = _flt.copy(); _flt.columns = [str(c).strip() for c in _flt.columns]
                        _c = { _ff_norm_header(c): c for c in _flt.columns }
                        _wh_col = _c.get("склад"); _pick_col = _c.get("выбрать")
                        if _wh_col and _pick_col:
                            _selected_wh = set(
                                _flt.loc[pd.to_numeric(_flt[_pick_col], errors="coerce").fillna(0).astype(int) == 1, _wh_col].astype(str)
                            )
                            logs.append("Фильтр складов: выбрано " + str(len(_selected_wh)) + " — " + ", ".join(sorted(_selected_wh))[:200])
                    except Exception:
                        pass
                if not _selected_wh and "Склад" in result_df.columns:
                    _selected_wh = set(result_df["Склад"].astype(str))
            except Exception:
                pass

            # 4) Квотирование IN-PLACE по группе SKU (WB→seller), сумма ≤ FF
            try:
                # Гарантируем наличие колонки результата
                if _ff_col not in result_df.columns:
                    result_df[_ff_col] = pd.to_numeric(result_df[_ff_base_col], errors="coerce").fillna(0).astype(float)
                # Служебные нормализованные ключи
                result_df["_wb_norm"] = result_df["Артикул WB"].map(_ff_norm_id) if "Артикул WB" in result_df.columns else None
                result_df["_seller_norm"] = result_df["Артикул продавца"].map(_ff_norm_id) if "Артикул продавца" in result_df.columns else None
                # Группа: WB если есть, иначе "S:<seller>"
                _key_series = result_df["_wb_norm"].where(
                    (result_df.get("_wb_norm").notna()) & (result_df.get("_wb_norm") != ""),
                    "S:" + result_df.get("_seller_norm").fillna("")
                )
                _limited_cnt = 0
                for _key, _grp in result_df.groupby(_key_series, dropna=False):
                    _wb_key = _key if (isinstance(_key, str) and not _key.startswith("S:")) else None
                    _seller_key = _key[2:] if (isinstance(_key, str) and _key.startswith("S:")) else None
                    _ff_total = None
                    if _wb_key: _ff_total = _ff_by_wb.get(_wb_key)
                    if _ff_total is None and _seller_key: _ff_total = _ff_by_seller.get(_seller_key)
                    _idx_all = _grp.index
                    _idx_sel = _idx_all if not _selected_wh else _grp.loc[_grp["Склад"].astype(str).isin(_selected_wh)].index
                    _base = pd.to_numeric(result_df.loc[_idx_sel, _ff_base_col], errors="coerce").fillna(0).clip(lower=0)
                    # Если FF нет или хватает — копируем базу
                    if (_ff_total is None) or (float(_base.sum()) <= float(_ff_total)):
                        result_df.loc[_idx_sel, _ff_col] = _base.astype(int)
                        continue
                    # Веса: Коэф. склада (>0) иначе доля базы
                    if "Коэф. склада" in result_df.columns:
                        _coefs = pd.to_numeric(result_df.loc[_idx_sel, "Коэф. склада"], errors="coerce").fillna(0).clip(lower=0)
                    else:
                        _coefs = pd.Series(1.0, index=_idx_sel, dtype="float64")
                    _sum_pos = float(_coefs[_coefs > 0].sum())
                    if _sum_pos > 0:
                        _weights = _coefs / _sum_pos
                    else:
                        _total_base = float(_base.sum()) or 1.0
                        _weights = _base / _total_base
                    _quotas = float(_ff_total) * _weights
                    _cap = pd.concat([_base, _quotas], axis=1).min(axis=1)  # дробные ограничения
                    _floor = _cap.fillna(0).astype(int)
                    # догоняем до min(FF, cap.sum()) по наибольшим остаткам
                    _target = int(min(float(_ff_total), float(_cap.sum())))
                    _rem = _target - int(_floor.sum())
                    if _rem > 0:
                        _frac = (_cap - _floor).sort_values(ascending=False)
                        for _j in list(_frac.index)[:_rem]:
                            _floor.at[_j] = _floor.at[_j] + 1
                    result_df.loc[_idx_sel, _ff_col] = _floor.astype(int)
                    _limited_cnt += 1
                # Чистим служебные колонки
                for _tmpcol in ("_wb_norm", "_seller_norm"):
                    if _tmpcol in result_df.columns:
                        try: result_df.drop(columns=[_tmpcol], inplace=True)
                        except Exception: pass
                logs.append(f"FF: групповой лимит применён — ограничены SKU={_limited_cnt}")
            except Exception as __e:
                logs.append(f"FF: критическая ошибка квотирования — {type(__e).__name__}")

        # ---------------- FF: один проход квотирования по SKU (после формирования result_df) ----------------
        # Хелперы локально (без asyncio):
        def _ff_norm_id(v):
            import math
            if v is None: return None
            if isinstance(v, float) and math.isnan(v): return None
            s = str(v).strip().replace(" ","")
            s = s.replace(" ","")
            if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
            return s or None
        def _ff_norm_wh(s):
            return str(s).strip().replace(" ","").lower()
        def _ff_read_sheet(xl, name: str) -> pd.DataFrame:
            try:
                df = xl.parse(name)
                df = df.copy()
                df.columns = [str(c).strip() for c in df.columns]
                return df
            except Exception:
                return pd.DataFrame()
        def _ff_build_maps(book_bytes: bytes):
            """Возвращает (ff_by_pair, ff_by_seller, ff_by_wb, selected_wh_norm, diag:list[str])."""
            from io import BytesIO
            diag = []
            ff_by_pair, ff_by_seller, ff_by_wb = {}, {}, {}
            selected_wh_norm = set()
            if not book_bytes:
                diag.append("FF: bytes=0 — квотирование пропущено.")
                return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh_norm, diag
            try:
                xl = pd.ExcelFile(BytesIO(book_bytes))
            except Exception:
                diag.append("FF: не удалось открыть входную книгу — квотирование пропущено.")
                return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh_norm, diag
            # Остатки Фулфилмент
            ff = _ff_read_sheet(xl, "Остатки Фулфилмент")
            if not ff.empty:
                cols = {str(c).strip().lower(): c for c in ff.columns}
                s_col = cols.get("артикул продавца") or cols.get("артикул поставщика") or cols.get("артикул")
                w_col = cols.get("артикул wb") or cols.get("артикул wildberries") or cols.get("артикул вб") or cols.get("код товара")
                q_col = cols.get("количество") or cols.get("остаток") or cols.get("кол-во") or cols.get("шт")
                if q_col and (s_col or w_col):
                    tmp = ff.copy()
                    if s_col in tmp: tmp[s_col] = tmp[s_col].map(_ff_norm_id)
                    if w_col in tmp: tmp[w_col] = tmp[w_col].map(_ff_norm_id)
                    tmp[q_col] = pd.to_numeric(tmp[q_col], errors="coerce").fillna(0).clip(lower=0)
                    diag.append(f"FF: строк={len(tmp)}, WB заполнено={int(tmp[w_col].notna().sum()) if w_col else 0}")
                    if s_col and w_col:
                        for (s,w), v in tmp.groupby([s_col,w_col], dropna=False)[q_col].sum().items():
                            ff_by_pair[(s,w)] = float(v)
                    if s_col:
                        for s, v in tmp.groupby([s_col], dropna=False)[q_col].sum().items():
                            ff_by_seller[s] = float(v)
                    if w_col:
                        for w, v in tmp.groupby([w_col], dropna=False)[q_col].sum().items():
                            ff_by_wb[w] = float(v)
            else:
                diag.append("FF: лист «Остатки Фулфилмент» пуст — квотирование пропущено.")
            # Склады для подсортировки
            flt = _ff_read_sheet(xl, "Склады для подсортировки")
            if not flt.empty:
                cols = {str(c).strip().lower(): c for c in flt.columns}
                wh_col = cols.get("склад"); pick_col = cols.get("выбрать")
                if wh_col and pick_col:
                    picked = flt.loc[pd.to_numeric(flt[pick_col], errors="coerce").fillna(0).astype(int) == 1, wh_col]
                    selected_wh_norm = set(_ff_norm_wh(x) for x in picked.astype(str))
                    diag.append(f"Фильтр складов: выбрано {len(selected_wh_norm)} — " + ", ".join(sorted(selected_wh_norm))[:200])
            return ff_by_pair, ff_by_seller, ff_by_wb, selected_wh_norm, diag

        def _ff_limit_once(df: pd.DataFrame, book_bytes: bytes, logs: list[str]) -> pd.DataFrame:
            """Пересчитывает df['Рекомендация с учётом остатков FF'] ровно один раз, опираясь на df['Рекомендация, шт']."""
            # Защита от повторного вызова
            try:
                if getattr(df, "attrs", None) and df.attrs.get("ff_applied") is True:
                    logs.append("FF: повторный вызов пропущен.")
                    return df
            except Exception:
                pass
            base_col, ff_col = "Рекомендация, шт", "Рекомендация с учётом остатков FF"
            need = ("Артикул продавца","Артикул WB","Склад")
            out = df.copy()
            if base_col not in out.columns or any(c not in out.columns for c in need):
                out[ff_col] = pd.to_numeric(out.get(base_col, 0), errors="coerce").fillna(0).astype(int)
                logs.append("FF: нет идентификаторов или базы — FF=базовой.")
                out.attrs["ff_applied"] = True
                return out
            # Карты FF и фильтр складов
            ff_by_pair, ff_by_seller, ff_by_wb, selected_wh_norm, diag = _ff_build_maps(book_bytes)
            logs.extend(diag)
            if not selected_wh_norm:
                selected_wh_norm = set(out["Склад"].map(_ff_norm_wh).unique())
            # Гарантируем колонку
            out[ff_col] = pd.to_numeric(out[base_col], errors="coerce").fillna(0).astype(int)
            # Нормализуем склад
            out["_ff_wh_norm"] = out["Склад"].map(_ff_norm_wh)
            import numpy as np, math
            limited_cnt = 0
            for (s_raw, w_raw), grp in out.groupby(["Артикул продавца","Артикул WB"], dropna=False):
                s = _ff_norm_id(s_raw); w = _ff_norm_id(w_raw)
                ff_total = ff_by_pair.get((s,w))
                if ff_total is None and w is not None:
                    ff_total = ff_by_wb.get(w)
                if ff_total is None and s is not None:
                    ff_total = ff_by_seller.get(s)
                idx_all = grp.index
                sel = grp.loc[grp["_ff_wh_norm"].isin(selected_wh_norm)].index
                if len(sel) == 0:
                    sel = idx_all
                base = pd.to_numeric(out.loc[sel, base_col], errors="coerce").fillna(0).clip(lower=0).astype(float)
                if (ff_total is None) or (float(base.sum()) <= float(ff_total)):
                    out.loc[sel, ff_col] = base.astype(int)
                    continue
                # квоты по «Коэф. склада» (>0) либо по долям базы
                if "Коэф. склада" in out.columns:
                    coefs = pd.to_numeric(out.loc[sel, "Коэф. склада"], errors="coerce").fillna(0).clip(lower=0).astype(float)
                else:
                    coefs = pd.Series(1.0, index=sel, dtype="float64")
                sum_pos = float(coefs[coefs > 0].sum())
                quota_raw = (float(ff_total) * (coefs / sum_pos)) if sum_pos > 0 else (float(ff_total) * (base / (float(base.sum()) or 1.0)))
                cap = pd.concat([base, quota_raw], axis=1).min(axis=1)  # дробные квоты
                floor_int = np.floor(cap).astype(int)
                # целевая сумма (не больше FF и не больше cap.sum())
                target = int(min(float(ff_total), float(cap.sum())))
                rem = target - int(floor_int.sum())
                if rem > 0:
                    frac = (cap - floor_int).sort_values(ascending=False)
                    take = list(frac.index[:rem])
                    floor_int.loc[take] = floor_int.loc[take] + 1
                out.loc[sel, ff_col] = floor_int.astype(int)
                limited_cnt += 1
            out.drop(columns=["_ff_wh_norm"], errors="ignore", inplace=True)
            out.attrs["ff_applied"] = True
            logs.append(f"FF: ограничены SKU={limited_cnt} (однократное квотирование).")
            return out

        # читаем байты входного Excel (seek → read → seek back)
        book_bytes: bytes = b""
        try:
            pos0 = await files[0].seek(0)
            book_bytes = await files[0].read()
            await files[0].seek(pos0 or 0)
        except Exception:
            book_bytes = b""
        # применяем ОДИН раз прямо к result_df
        try:
            result_df = _ff_limit_once(result_df, book_bytes, logs)
        except Exception as _e:
            logs.append(f"FF: критическая ошибка квотирования — {type(_e).__name__}")

        # 5) Если используется фиксированный список колонок для выгрузки — добавим наш столбец
        try:
            for _n, _v in list(locals()).items():
                if isinstance(_v, (list, tuple)) and any(isinstance(x, str) for x in _v):
                    if ("Рекомендация, шт" in _v) and ("Рекомендация с учётом остатков FF" not in _v):
                        locals()[_n] = list(_v) + ["Рекомендация с учётом остатков FF"]
        except Exception:
            pass
        return {"ok": True, "log": "\n".join(logs), "download_token": token}
    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse({"ok": False, "log": "\n".join(logs + [f'Ошибка: {e}', 'TRACEBACK:', tb])}, status_code=500)
