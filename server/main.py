from __future__ import annotations

import io
import re
import secrets
from pathlib import Path
from threading import Lock
from typing import List

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from engine.export_prototype import build_prototype_workbook
from engine.transform import (
    sales_by_warehouse_from_details,
    stock_snapshot_from_details_or_daily,
)
from wb_io.wb_readers import read_stock_history


app = FastAPI(title="WB Order Engine")
_base_dir = Path(__file__).resolve().parent
_templates = Jinja2Templates(directory=str(_base_dir / "templates"))
_static_dir = _base_dir / "static"
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

_SALES_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Заказали, шт"]
_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Остаток"]

_DOWNLOADS: dict[str, bytes] = {}
_DOWNLOADS_LOCK = Lock()
_DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}")


def _latest_date_label(df: pd.DataFrame) -> str | None:
    candidates: list[tuple[pd.Timestamp, str]] = []
    for column in df.columns:
        column_str = str(column).strip()
        if not _DATE_PATTERN.fullmatch(column_str):
            continue
        try:
            parsed = pd.to_datetime(column_str, format="%d.%m.%Y")
        except ValueError:
            continue
        candidates.append((parsed, column_str))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return _templates.TemplateResponse("index.html", {"request": request})


@app.post("/build")
async def build(files: List[UploadFile] = File(...)) -> JSONResponse:
    if not files:
        return JSONResponse({"ok": False, "log": ["Не переданы файлы."], "download_token": None})

    log_messages: list[str] = []
    sales_parts: list[pd.DataFrame] = []
    stock_parts: list[pd.DataFrame] = []

    for upload in files:
        filename = upload.filename or "без имени"
        log_messages.append(f"Файл «{filename}»: начало обработки.")
        try:
            content = await upload.read()
        except Exception as exc:
            log_messages.append(f"Файл «{filename}»: не удалось прочитать ({exc}).")
            continue

        if not content:
            log_messages.append(f"Файл «{filename}»: пустой файл.")
            continue

        try:
            sheets = read_stock_history(content, filename=upload.filename)
        except Exception as exc:
            log_messages.append(f"Файл «{filename}»: ошибка разбора ({exc}).")
            continue

        if not sheets:
            log_messages.append(f"Файл «{filename}»: отчёт не распознан.")
            continue

        detail_df = sheets.get("Детальная информация")
        daily_df = sheets.get("Остатки по дням")

        if detail_df is not None:
            sales_df = sales_by_warehouse_from_details(detail_df)
            log_messages.append(
                f"Файл «{filename}»: найдено {len(sales_df)} строк продаж по складам."
            )
            if not sales_df.empty:
                sales_parts.append(sales_df)
        else:
            log_messages.append(f"Файл «{filename}»: лист «Детальная информация» не найден.")

        stock_source = daily_df if daily_df is not None else detail_df
        if stock_source is not None:
            stock_df = stock_snapshot_from_details_or_daily(stock_source)
            latest_label = _latest_date_label(stock_source)
            if latest_label:
                log_messages.append(
                    f"Файл «{filename}»: остатки определены по дате {latest_label}."
                )
            log_messages.append(
                f"Файл «{filename}»: сформировано {len(stock_df)} строк остатков."
            )
            if not stock_df.empty:
                stock_parts.append(stock_df)
        else:
            log_messages.append(
                f"Файл «{filename}»: лист с остатками не найден, пропуск расчёта остатков."
            )

    if not sales_parts and not stock_parts:
        log_messages.append("Не удалось собрать данные для продаж и остатков.")
        return JSONResponse({"ok": False, "log": log_messages, "download_token": None})

    sales_result = (
        pd.concat(sales_parts, ignore_index=True) if sales_parts else pd.DataFrame(columns=_SALES_COLUMNS)
    )
    stock_result = (
        pd.concat(stock_parts, ignore_index=True) if stock_parts else pd.DataFrame(columns=_STOCK_COLUMNS)
    )

    workbook = build_prototype_workbook(sales_result, stock_result)
    token = secrets.token_urlsafe(16)
    with _DOWNLOADS_LOCK:
        _DOWNLOADS[token] = workbook.getvalue()

    log_messages.append("Валидационный инпут собран, можно скачивать Excel.")
    return JSONResponse({"ok": True, "log": log_messages, "download_token": token})


@app.get("/download/{token}")
async def download(token: str) -> StreamingResponse:
    with _DOWNLOADS_LOCK:
        content = _DOWNLOADS.pop(token, None)

    if content is None:
        raise HTTPException(status_code=404, detail="Файл не найден или ссылка устарела.")

    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=\"Input_Prototype_Filled.xlsx\""},
    )
