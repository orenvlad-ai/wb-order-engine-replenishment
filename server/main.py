from __future__ import annotations
from io import BytesIO
import secrets
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wb_io.wb_readers import read_stock_history, read_stock_snapshot
from engine.transform import (
    sales_by_warehouse_from_details,
    merge_sales_with_stock_today,
    stock_from_snapshot,
)
from engine.export_prototype import build_prototype_workbook

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

@app.post("/build")
async def build(files: List[UploadFile] = File(...)):
    logs: List[str] = []
    combined_frames: List[pd.DataFrame] = []

    if not files:
        return JSONResponse({"ok": False, "log": "Файлы не переданы"}, status_code=400)

    try:
        for f in files:
            raw = await f.read()
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

            sheets = read_stock_history(raw, f.filename)
            if not sheets:
                logs.append(f"{f.filename}: источник не распознан")
                continue

            detail = sheets.get("Детальная информация")
            daily = sheets.get("Остатки по дням")

            df_sales = sales_by_warehouse_from_details(detail) if detail is not None else pd.DataFrame()
            merged = merge_sales_with_stock_today(df_sales, detail, daily)
            if not merged.empty:
                combined_frames.append(merged)

            logs.append(
                f"{f.filename}: источник «История остатков» — {len(merged)} строк"
            )

        if combined_frames:
            sales_stock = pd.concat(combined_frames, ignore_index=True).reindex(columns=_SALES_STOCK_COLUMNS)
        else:
            sales_stock = pd.DataFrame(columns=_SALES_STOCK_COLUMNS)

        logs.append(
            f"Итог: «Продажи и остатки по складам» — {len(sales_stock)}."
        )

        bio: BytesIO = build_prototype_workbook(sales_stock)
        token = secrets.token_urlsafe(16)
        _memory_artifacts[token] = bio.getvalue()

        return {"ok": True, "log": "\n".join(logs), "download_token": token}

    except Exception as e:
        return JSONResponse({"ok": False, "log": "\n".join(logs + [f'Ошибка: {e}'])}, status_code=500)

@app.get("/download/{token}")
async def download(token: str):
    data = _memory_artifacts.pop(token, None)
    if not data:
        return JSONResponse({"ok": False, "log": "Файл не найден или срок истёк"}, status_code=404)
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="Input_Prototype_Filled.xlsx"'},
    )
