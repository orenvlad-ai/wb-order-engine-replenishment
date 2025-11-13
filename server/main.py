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
                if k in cols: return cols[k]
            return None
        c_seller = pick(["артикул продавца"])
        c_wb     = pick(["артикул wb","артикул wb."])
        c_wh     = pick(["склад"])
        c_avg    = pick(["средние продажи в день","средние продажи/день","средние продажи"])
        needed = [c_seller, c_wb, c_wh, c_avg]
        if any(c is None for c in needed):
            return JSONResponse({"ok": False, "log": "В листе «Продажи по складам» отсутствуют нужные колонки"}, status_code=400)
        df_out = pd.DataFrame({
            "Артикул продавца": df[c_seller],
            "Артикул WB": df[c_wb],
            "Склад": df[c_wh],
            "Средние продажи в день": pd.to_numeric(df[c_avg], errors="coerce").fillna(0.0),
        })
        # черновая формула: реком. заказ = ceil(avg_day * 10)
        df_out["Реком. заказ, шт"] = df_out["Средние продажи в день"].apply(lambda x: int(math.ceil(float(x)*10.0)))
        # сформировать Excel с рекомендациями
        out = BytesIO()
        try:
            writer = pd.ExcelWriter(out, engine="xlsxwriter")
        except Exception:
            writer = pd.ExcelWriter(out, engine="openpyxl")
        with writer:
            df_out.to_excel(writer, sheet_name="Рекомендации", index=False)
        out.seek(0)
        token = secrets.token_urlsafe(16)
        _memory_artifacts[token] = {
            "data": out.getvalue(),
            "filename": "WB_Replenishment_Recommendations.xlsx"
        }
        logs.append(f"Рекомендации сформированы: {len(df_out)} строк")
        return {"ok": True, "log": "\n".join(logs), "download_token": token}
    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse({"ok": False, "log": "\n".join(logs + [f'Ошибка: {e}', 'TRACEBACK:', tb])}, status_code=500)
