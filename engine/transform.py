from __future__ import annotations

import re
from typing import List, Optional

import pandas as pd


_SALES_COLUMNS = ["Склад", "Артикул продавца", "Артикул WB", "Заказали, шт"]
_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Остаток"]
_SALES_STOCK_COLUMNS = [
    "Артикул продавца",
    "Артикул WB",
    "Склад",
    "Заказали, шт",
    "Остаток на сегодня",
]


_STOCK_TODAY_ALIASES = [
    "Остатки на текущий день",
    "Остаток на текущий день",
    "Остатки на сегодня",
    "Остаток на сегодня",
]


def _find_column(df: pd.DataFrame, target: str) -> Optional[str]:
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


def _find_stock_today_column(df: pd.DataFrame) -> Optional[str]:
    for alias in _STOCK_TODAY_ALIASES:
        column = _find_column(df, alias)
        if column is not None:
            return column
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for normalized_name, original in normalized.items():
        if "остат" in normalized_name and ("сегодня" in normalized_name or "текущ" in normalized_name):
            return original
    return None


def _drop_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = ~df.apply(lambda series: series.astype(str).str.strip().str.lower() == "итого").any(axis=1)
    return df[mask]


def _to_numeric(series: pd.Series) -> pd.Series:
    prepared = (
        series.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(prepared, errors="coerce")


def sales_by_warehouse_from_details(df: pd.DataFrame) -> pd.DataFrame:
    data = _prepare_required_columns(df, _SALES_COLUMNS)
    data = _drop_totals(data)
    if "Заказали, шт" in data.columns:
        data["Заказали, шт"] = _to_numeric(data["Заказали, шт"]).fillna(0)
    return data.reset_index(drop=True)


def _normalize_key(value) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def merge_sales_with_stock_today(
    sales_df: pd.DataFrame,
    detail_df: Optional[pd.DataFrame],
    daily_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    if sales_df is None or sales_df.empty:
        return pd.DataFrame(columns=_SALES_STOCK_COLUMNS)

    result = sales_df.copy()
    for column in ["Артикул продавца", "Артикул WB", "Склад", "Заказали, шт"]:
        if column not in result.columns:
            result[column] = pd.Series(dtype="object")
    result = result[["Артикул продавца", "Артикул WB", "Склад", "Заказали, шт"]]
    result["Склад"] = result["Склад"].fillna("-").replace("", "-")
    if "Заказали, шт" in result.columns:
        result["Заказали, шт"] = _to_numeric(result["Заказали, шт"]).fillna(0)

    result["Остаток на сегодня"] = pd.Series([pd.NA] * len(result))

    detail_stock_filled = False
    keys = ["Артикул продавца", "Артикул WB", "Склад"]
    if detail_df is not None and not detail_df.empty:
        detail_prepared = _prepare_required_columns(detail_df, keys)
        detail_prepared["Склад"] = detail_prepared["Склад"].fillna("-").replace("", "-")
        stock_column = _find_stock_today_column(detail_df)
        if stock_column is not None and stock_column in detail_df.columns:
            detail_prepared["Остаток на сегодня"] = _to_numeric(detail_df[stock_column]).fillna(0)
            detail_stock_filled = True
        detail_prepared = _drop_totals(detail_prepared)
        if "Остаток на сегодня" not in detail_prepared.columns:
            detail_prepared["Остаток на сегодня"] = pd.Series(dtype="float")
        detail_prepared = detail_prepared.drop_duplicates(subset=keys, keep="last")
        if detail_stock_filled:
            detail_stock_df = detail_prepared[[*keys, "Остаток на сегодня"]]
            result = result.merge(detail_stock_df, on=keys, how="left", suffixes=("", "_detail"))
            if "Остаток на сегодня_detail" in result.columns:
                result["Остаток на сегодня"] = result["Остаток на сегодня_detail"]
                result = result.drop(columns=["Остаток на сегодня_detail"])

    if (not detail_stock_filled) and daily_df is not None and not daily_df.empty:
        latest_column = _pick_latest_date_column(daily_df)
        if latest_column is not None and latest_column in daily_df.columns:
            daily_prepared = _prepare_required_columns(daily_df, ["Артикул продавца", "Артикул WB"])
            daily_prepared["Остаток на сегодня"] = _to_numeric(daily_df[latest_column]).fillna(0)
            daily_prepared = _drop_totals(daily_prepared)
            daily_prepared = daily_prepared.drop_duplicates(
                subset=["Артикул WB", "Артикул продавца"], keep="last"
            )

            mapping_wb = {}
            mapping_seller = {}
            for _, row in daily_prepared.iterrows():
                wb_key = _normalize_key(row.get("Артикул WB"))
                seller_key = _normalize_key(row.get("Артикул продавца"))
                stock_value = row.get("Остаток на сегодня")
                if wb_key is not None:
                    mapping_wb[wb_key] = stock_value
                elif seller_key is not None:
                    mapping_seller[seller_key] = stock_value

            mask = result["Остаток на сегодня"].isna()
            if mapping_wb:
                wb_normalized = result.loc[mask, "Артикул WB"].map(_normalize_key)
                result.loc[mask, "Остаток на сегодня"] = wb_normalized.map(mapping_wb)
                mask = result["Остаток на сегодня"].isna()
            if mapping_seller:
                seller_normalized = result.loc[mask, "Артикул продавца"].map(_normalize_key)
                result.loc[mask, "Остаток на сегодня"] = seller_normalized.map(mapping_seller)

    result["Остаток на сегодня"] = _to_numeric(result["Остаток на сегодня"]).fillna(0)
    result["Склад"] = result["Склад"].fillna("-").replace("", "-")
    return result[_SALES_STOCK_COLUMNS].reset_index(drop=True)


_DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}")


def _pick_latest_date_column(df: pd.DataFrame) -> Optional[str]:
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
