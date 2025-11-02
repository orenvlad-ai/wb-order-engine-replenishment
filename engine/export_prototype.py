from io import BytesIO
from typing import Iterable

import pandas as pd

_SALES_SHEET = "Продажи по складам"
_STOCK_SHEET = "Остатки на сегодня"
_SUPPLY_SHEET = "Поставки в пути"
_MIN_STOCK_SHEET = "MinStock"
_THRESHOLD_SHEET = "Порог загрузки транспорта"

_SALES_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Заказали, шт"]
_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Остаток"]
_SUPPLY_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Количество"]
_MIN_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Значение"]
_THRESHOLD_COLUMNS = ["Порог загрузки, шт"]


def _ensure_columns(dataframe: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = dataframe.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = pd.Series(dtype="object")
    return df[list(columns)] if df.size else df.reindex(columns=columns)


def build_prototype_workbook(sales_df: pd.DataFrame, stock_df: pd.DataFrame) -> BytesIO:
    sales = _ensure_columns(sales_df, _SALES_COLUMNS)
    stock = _ensure_columns(stock_df, _STOCK_COLUMNS)
    supplies = pd.DataFrame(columns=_SUPPLY_COLUMNS)
    min_stock = pd.DataFrame(columns=_MIN_STOCK_COLUMNS)
    threshold = pd.DataFrame(columns=_THRESHOLD_COLUMNS)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        sales.to_excel(writer, sheet_name=_SALES_SHEET, index=False)
        stock.to_excel(writer, sheet_name=_STOCK_SHEET, index=False)
        supplies.to_excel(writer, sheet_name=_SUPPLY_SHEET, index=False)
        min_stock.to_excel(writer, sheet_name=_MIN_STOCK_SHEET, index=False)
        threshold.to_excel(writer, sheet_name=_THRESHOLD_SHEET, index=False)
    buffer.seek(0)
    return buffer
