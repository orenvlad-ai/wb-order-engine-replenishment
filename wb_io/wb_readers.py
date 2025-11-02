import io
from typing import Dict

import pandas as pd


_DETAIL_REQUIRED = ["Склад", "Артикул продавца", "Артикул WB", "Заказали, шт"]
_DAILY_REQUIRED = ["Артикул продавца", "Артикул WB"]
_TARGET_SHEETS = {
    "детальная информация": ("Детальная информация", _DETAIL_REQUIRED),
    "остатки по дням": ("Остатки по дням", _DAILY_REQUIRED),
}


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    return df


def _row_contains_all_required(row: pd.Series, required: list[str]) -> bool:
    values = {str(v).strip() for v in row if pd.notna(v)}
    return all(req in values for req in required)


def _ensure_header(df: pd.DataFrame, required: list[str]) -> pd.DataFrame:
    if df.empty:
        return df

    columns = [str(col).strip() for col in df.columns]
    if all(req in columns for req in required):
        df.columns = columns
        return df

    first_row = df.iloc[0]
    if _row_contains_all_required(first_row, required):
        df = df.iloc[1:].copy()
        df.columns = [str(v).strip() for v in first_row]
        return df

    df.columns = columns
    return df


def _match_target_sheet(sheet_name: str) -> tuple[str, list[str]] | None:
    key = sheet_name.strip().lower()
    return _TARGET_SHEETS.get(key)


def _load_excel(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    excel = pd.ExcelFile(io.BytesIO(file_bytes))
    sheets: Dict[str, pd.DataFrame] = {}
    for original_name in excel.sheet_names:
        df = excel.parse(original_name)
        df = _clean_dataframe(df)
        target = _match_target_sheet(original_name)
        if target:
            normalized_name, required = target
            df = _ensure_header(df, required)
            sheets[normalized_name] = df
    return sheets


def _try_read_csv(file_bytes: bytes) -> pd.DataFrame | None:
    encodings = [None, "utf-8", "utf-16", "cp1251"]
    delimiters = [";", ",", "\t"]
    for encoding in encodings:
        for delimiter in delimiters:
            try:
                buffer = io.BytesIO(file_bytes)
                df = pd.read_csv(buffer, encoding=encoding, sep=delimiter)
                if df.empty:
                    continue
                return _clean_dataframe(df)
            except Exception:
                continue
    return None


def read_stock_history(file_bytes: bytes, filename: str | None = None) -> Dict[str, pd.DataFrame]:
    """Прочитать отчёт «История остатков» и вернуть релевантные листы."""

    if not file_bytes:
        return {}

    try:
        sheets = _load_excel(file_bytes)
        if sheets:
            return sheets
    except Exception:
        sheets = {}

    csv_df = _try_read_csv(file_bytes)
    if csv_df is None:
        return {}

    csv_df = _ensure_header(csv_df, _DETAIL_REQUIRED)
    return {"Детальная информация": csv_df}
