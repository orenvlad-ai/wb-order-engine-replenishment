from __future__ import annotations
from io import BytesIO
import secrets
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wb_io.wb_readers import read_stock_history
from engine.transform import (
    sales_by_warehouse_from_details,
    stock_snapshot_from_details_or_daily,
)
from engine.export_prototype import build_prototype_workbook

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
    sales_frames: List[pd.DataFrame] = []
    stock_frames: List[pd.DataFrame] = []

    if not files:
        return JSONResponse({"ok": False, "log": "Файлы не переданы"}, status_code=400)

    try:
        for f in files:
            raw = await f.read()
            sheets = read_stock_history(raw, f.filename)
            if not sheets:
                logs.append(f"{f.filename}: не распознаны листы")
                continue

            logs.append(f"{f.filename}: распознаны листы — {', '.join(sheets.keys())}")

            detail = sheets.get("Детальная информация")
            daily = sheets.get("Остатки по дням")

            if detail is not None:
                df_sales = sales_by_warehouse_from_details(detail)
                if not df_sales.empty:
                    sales_frames.append(df_sales)

            if daily is not None:
                df_stock = stock_snapshot_from_details_or_daily(daily)
                if not df_stock.empty:
                    stock_frames.append(df_stock)

        sales = pd.concat(sales_frames, ignore_index=True) if sales_frames else pd.DataFrame(
            columns=["Артикул продавца","Артикул WB","Склад","Заказали, шт"]
        )
        stock = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame(
            columns=["Артикул продавца","Артикул WB","Остаток"]
        )

        logs.append(f"Итог: «Продажи по складам» — {len(sales)}; «Остатки на сегодня» — {len(stock)}.")

        bio: BytesIO = build_prototype_workbook(sales, stock)
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
