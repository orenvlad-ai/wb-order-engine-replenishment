import re
from typing import List

import pandas as pd


_SALES_COLUMNS = ["Склад", "Артикул продавца", "Артикул WB", "Заказали, шт"]
_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Остаток"]


def _find_column(df: pd.DataFrame, target: str) -> str | None:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    return normalized.get(target.strip().lower())


def _prepare_required_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)

    prepared = {}
    length = len(df)
    for column in columns:
        source = _find_column(df, column)
        if source is None:
            prepared[column] = [None] * length
        else:
            prepared[column] = df[source]
    return pd.DataFrame(prepared)


def _drop_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = ~df.apply(lambda series: series.astype(str).str.strip().str.lower() == "итого").any(axis=1)
    return df[mask]


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace("\xa0", "", regex=False).str.replace(" ", "", regex=False), errors="coerce")


def sales_by_warehouse_from_details(df: pd.DataFrame) -> pd.DataFrame:
    data = _prepare_required_columns(df, _SALES_COLUMNS)
    data = _drop_totals(data)
    if "Заказали, шт" in data.columns:
        data["Заказали, шт"] = _to_numeric(data["Заказали, шт"]).fillna(0)
    return data.reset_index(drop=True)


_DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}")


def _pick_latest_date_column(df: pd.DataFrame) -> str | None:
    candidates = []
    for column in df.columns:
        column_str = str(column).strip()
        if _DATE_PATTERN.fullmatch(column_str):
            try:
                candidates.append((pd.to_datetime(column_str, format="%d.%m.%Y"), column))
            except ValueError:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def stock_snapshot_from_details_or_daily(df: pd.DataFrame) -> pd.DataFrame:
    base_columns = _STOCK_COLUMNS[:-2]
    data = _prepare_required_columns(df, base_columns)
    latest_column = _pick_latest_date_column(df)
    if latest_column is None:
        return pd.DataFrame(columns=_STOCK_COLUMNS)

    stock_series = _to_numeric(df[latest_column])
    data["Остаток"] = stock_series.fillna(0)
    data["Склад"] = "-"
    data = _drop_totals(data)
    return data[_STOCK_COLUMNS].reset_index(drop=True)


def stock_from_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    data = _prepare_required_columns(df, _STOCK_COLUMNS)
    if "Остаток" in data.columns:
        data["Остаток"] = _to_numeric(data["Остаток"]).fillna(0)
    data = _drop_totals(data)
    return data[_STOCK_COLUMNS].reset_index(drop=True)
