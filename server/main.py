from __future__ import annotations

import asyncio
from collections import deque
from io import BytesIO
from typing import Deque, Dict, List
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from engine.export_prototype import build_prototype_workbook
from engine.transform import (
    sales_by_warehouse_from_details,
    stock_snapshot_from_details_or_daily,
)
from wb_io.wb_readers import read_stock_history

app = FastAPI()
templates = Jinja2Templates(directory="server/templates")

# Ограничение на количество сохранённых файлов в памяти, чтобы не раздувать хранилище.
MAX_STORED_WORKBOOKS = 20
_generated_workbooks: Dict[str, BytesIO] = {}
_generation_order: Deque[str] = deque()
_lock = asyncio.Lock()


async def _store_workbook(token: str, workbook: BytesIO) -> None:
    async with _lock:
        if token not in _generated_workbooks:
            if len(_generation_order) >= MAX_STORED_WORKBOOKS:
                oldest = _generation_order.popleft()
                _generated_workbooks.pop(oldest, None)
            _generation_order.append(token)
        _generated_workbooks[token] = workbook


async def _get_workbook(token: str) -> BytesIO | None:
    async with _lock:
        return _generated_workbooks.get(token)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/build")
async def build_validation_input(files: List[UploadFile] = File(...)) -> JSONResponse:
    if not files:
        raise HTTPException(status_code=400, detail="Не переданы файлы")

    sales_frames: List[pd.DataFrame] = []
    stock_frames: List[pd.DataFrame] = []
    log: List[str] = []

    for upload in files:
        content = await upload.read()
        filename = upload.filename or "Безымянный файл"

        try:
            sheets = read_stock_history(content, filename)
        except Exception as exc:  # noqa: BLE001 - важно зафиксировать ошибку чтения
            log.append(f"{filename}: не удалось прочитать файл ({exc})")
            continue

        if not sheets:
            log.append(f"{filename}: листы не найдены")
            continue

        log.append(f"{filename}: найдено листов {len(sheets)}")

        for title, df in sheets.items():
            sheet_name = title.strip()
            log.append(f"— «{sheet_name}»: {len(df)} строк")

            if sheet_name.lower().startswith("детал"):
                try:
                    sales_df = sales_by_warehouse_from_details(df)
                except Exception as exc:  # noqa: BLE001
                    log.append(f"    Ошибка обработки продаж: {exc}")
                    continue

                if not sales_df.empty:
                    sales_frames.append(sales_df)
                    log.append(f"    Продажи: {len(sales_df)} строк")
                else:
                    log.append("    Продажи: нет данных")

            if sheet_name.lower().startswith("остат"):
                try:
                    stock_df = stock_snapshot_from_details_or_daily(df)
                except Exception as exc:  # noqa: BLE001
                    log.append(f"    Ошибка обработки остатков: {exc}")
                    continue

                if not stock_df.empty:
                    stock_frames.append(stock_df)
                    log.append(f"    Остатки: {len(stock_df)} строк")
                else:
                    log.append("    Остатки: нет данных")

    if not sales_frames and not stock_frames:
        raise HTTPException(status_code=400, detail="Не удалось собрать данные из файлов")

    sales_df = _concat_frames(sales_frames)
    stock_df = _concat_frames(stock_frames)

    log.append(
        "Итог: вкладка «Продажи по складам» — {sales} строк, «Остатки на сегодня» — {stock} строк.".format(
            sales=len(sales_df),
            stock=len(stock_df),
        )
    )

    workbook = build_prototype_workbook(sales_df, stock_df)
    workbook.seek(0)
    download_token = uuid4().hex
    await _store_workbook(download_token, workbook)

    return JSONResponse({
        "ok": True,
        "log": log,
        "download_token": download_token,
    })


@app.get("/download/{token}")
async def download_workbook(token: str) -> StreamingResponse:
    workbook = await _get_workbook(token)
    if workbook is None:
        raise HTTPException(status_code=404, detail="Файл не найден или срок действия истёк")

    workbook.seek(0)
    headers = {"Content-Disposition": "attachment; filename=Input_Prototype_Filled.xlsx"}
    return StreamingResponse(workbook, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)


def _concat_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
