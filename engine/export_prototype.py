from io import BytesIO
from typing import Iterable, Optional

import pandas as pd

_SALES_SHEET = "Продажи по складам"
_SUPPLY_SHEET = "Поставки в пути"
_FULFILLMENT_SHEET = "Остатки Фулфилмент"
_MIN_STOCK_SHEET = "MinStock"
_THRESHOLD_SHEET = "Порог загрузки транспорта"
_ACCEPTANCE_SHEET = "Окна приёмки"
_STOCK_DAILY_SHEET = "История остатков по дням"
_STOCK_DAILY_ID_COLS = ["Артикул продавца", "Артикул WB"]
_STOCK_DAILY_COLUMNS = [
    "Артикул продавца",
    "Артикул WB",
    "Остаток на сегодня",
]

_SALES_STOCK_COLUMNS = [
    "Артикул продавца",
    "Артикул WB",
    "Склад",
    "Заказали, шт",
    "Дней в наличии",
    "Средние продажи в день",
    "Коэф. склада",
]
_SUPPLY_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Количество"]
_FULFILLMENT_COLUMNS = ["Артикул продавца", "Артикул WB", "Количество"]
_MIN_STOCK_COLUMNS = ["Артикул продавца", "Артикул WB", "Значение"]
_THRESHOLD_COLUMNS = ["Порог загрузки, шт"]
_ACCEPTANCE_COLUMNS = ["Название склада", "Количество дней"]


def _ensure_columns(
    dataframe: Optional[pd.DataFrame], columns: Iterable[str]
) -> pd.DataFrame:
    if dataframe is None:
        return pd.DataFrame(columns=columns)

    df = dataframe.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = pd.Series(dtype="object")
    return df.loc[:, list(columns)] if df.size else df.reindex(columns=columns)


def _prepare_daily_sheet(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame(columns=_STOCK_DAILY_ID_COLS)
    out = df.copy()
    for c in _STOCK_DAILY_ID_COLS:
        if c not in out.columns:
            out[c] = pd.Series(dtype="object")
    other = [c for c in out.columns if c not in _STOCK_DAILY_ID_COLS]
    return out[_STOCK_DAILY_ID_COLS + other]


def build_prototype_workbook(
    sales_stock_df: Optional[pd.DataFrame] = None,
    supplies_df: Optional[pd.DataFrame] = None,
    fulfillment_df: Optional[pd.DataFrame] = None,
    min_stock_df: Optional[pd.DataFrame] = None,
    threshold_df: Optional[pd.DataFrame] = None,
    acceptance_df: Optional[pd.DataFrame] = None,
    daily_stock_df: Optional[pd.DataFrame] = None,
) -> BytesIO:
    # ── Расчёты для листа «Продажи по складам» ───────────────────────────────────
    base_cols = ["Артикул продавца", "Артикул WB", "Склад", "Заказали, шт"]
    sales_base = _ensure_columns(sales_stock_df, base_cols)

    sku_sum = (
        sales_base.groupby(
            ["Артикул продавца", "Артикул WB"], dropna=False, as_index=False
        )["Заказали, шт"].sum()
    ).rename(columns={"Заказали, шт": "ΣПродаж"})
    sales_enriched = sales_base.merge(
        sku_sum, on=["Артикул продавца", "Артикул WB"], how="left"
    )
    sales_enriched["ΣПродаж"] = sales_enriched["ΣПродаж"].fillna(0)

    denom_sum = sales_enriched["ΣПродаж"].replace(0, pd.NA)
    sales_enriched["Коэф. склада"] = (
        sales_enriched["Заказали, шт"] / denom_sum
    ).fillna(0)

    days_df = None
    if isinstance(daily_stock_df, pd.DataFrame) and not daily_stock_df.empty:
        id_cols = ["Артикул продавца", "Артикул WB"]
        date_cols = [c for c in daily_stock_df.columns if c not in id_cols]
        if date_cols:
            tmp = daily_stock_df.copy()
            tmp["Дней в наличии"] = (
                tmp[date_cols].fillna(0) > 0
            ).sum(axis=1).astype(int)
            days_df = tmp[id_cols + ["Дней в наличии"]]
    if days_df is not None:
        sales_enriched = sales_enriched.merge(
            days_df, on=["Артикул продавца", "Артикул WB"], how="left"
        )
    else:
        sales_enriched["Дней в наличии"] = 0
    sales_enriched["Дней в наличии"] = (
        sales_enriched["Дней в наличии"].fillna(0).astype(int)
    )

    denom_days = sales_enriched["Дней в наличии"].replace(0, pd.NA)
    avg_total_per_day = (sales_enriched["ΣПродаж"] / denom_days).fillna(0)
    sales_enriched["Средние продажи в день"] = (
        avg_total_per_day * sales_enriched["Коэф. склада"]
    ).fillna(0)

    sales_out = _ensure_columns(sales_enriched, _SALES_STOCK_COLUMNS)

    sheets = [
        (_SALES_SHEET, sales_out),
        (_SUPPLY_SHEET, _ensure_columns(supplies_df, _SUPPLY_COLUMNS)),
        (
            _FULFILLMENT_SHEET,
            _ensure_columns(fulfillment_df, _FULFILLMENT_COLUMNS),
        ),
        (_MIN_STOCK_SHEET, _ensure_columns(min_stock_df, _MIN_STOCK_COLUMNS)),
        (_THRESHOLD_SHEET, _ensure_columns(threshold_df, _THRESHOLD_COLUMNS)),
        (_ACCEPTANCE_SHEET, _ensure_columns(acceptance_df, _ACCEPTANCE_COLUMNS)),
        # История остатков по дням: ID + все даты из отчёта (без «Остаток на сегодня»)
        (_STOCK_DAILY_SHEET, _prepare_daily_sheet(daily_stock_df)),
    ]

    buffer = BytesIO()
    try:
        writer = pd.ExcelWriter(buffer, engine="xlsxwriter")
    except ImportError:
        writer = pd.ExcelWriter(buffer, engine="openpyxl")

    with writer:
        for sheet_name, dataframe in sheets:
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)

    buffer.seek(0)
    return buffer
