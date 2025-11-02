import io
from typing import Dict, Iterable, List, Optional

import pandas as pd
from openpyxl import load_workbook


_DETAIL_REQUIRED = ["Склад", "Артикул продавца", "Артикул WB", "Заказали, шт"]
_DAILY_REQUIRED = ["Артикул продавца", "Артикул WB"]
_SNAPSHOT_REQUIRED = ["Артикул продавца", "Артикул WB", "Склад", "Остаток"]
_SNAPSHOT_ALIASES = {
    "Артикул продавца": {
        "артикул продавца",
        "артикул поставщика",
        "арт продавца",
    },
    "Артикул WB": {
        "артикул wb",
        "арт wb",
        "артикул wildberries",
        "артикул wb.",
    },
    "Склад": {
        "склад",
        "склады",
        "склад поставки",
    },
    "Остаток": {
        "остаток",
        "остатки",
        "остаток на складе",
        "доступный остаток",
        "количество",
        "в наличии",
    },
}
_TARGET_SHEETS = {
    "детальная информация": ("Детальная информация", _DETAIL_REQUIRED),
    "остатки по дням": ("Остатки по дням", _DAILY_REQUIRED),
}


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    return df


def _normalize_header_value(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").replace("\n", " ").replace("\r", " ")
    text = text.strip().strip(":")
    text = " ".join(text.lower().split())
    return text


def _match_aliases(row: Iterable[object], aliases: Dict[str, set[str]]) -> Optional[Dict[str, int]]:
    normalized = [_normalize_header_value(value) for value in row]
    mapping: Dict[str, int] = {}
    for index, value in enumerate(normalized):
        if not value:
            continue
        for target, options in aliases.items():
            if target in mapping:
                continue
            if value in options:
                mapping[target] = index
                break
    if len(mapping) == len(aliases):
        return mapping
    return None


def _build_snapshot_dataframe(rows: List[List[object]]) -> Optional[pd.DataFrame]:
    if not rows:
        return None

    for idx, row in enumerate(rows):
        mapping = _match_aliases(row, _SNAPSHOT_ALIASES)
        if not mapping:
            continue

        records: List[Dict[str, object]] = []
        for data_row in rows[idx + 1 :]:
            if not data_row or all(cell in (None, "") for cell in data_row):
                continue
            record: Dict[str, object] = {}
            empty = True
            for target in _SNAPSHOT_REQUIRED:
                position = mapping.get(target)
                value = data_row[position] if position is not None and position < len(data_row) else None
                if isinstance(value, str):
                    value = value.strip()
                if value not in (None, ""):
                    empty = False
                record[target] = value
            if empty:
                continue
            records.append(record)

        if not records:
            return pd.DataFrame(columns=_SNAPSHOT_REQUIRED)
        return pd.DataFrame(records)

    return None


def _load_snapshot_excel(file_bytes: bytes) -> Optional[pd.DataFrame]:
    try:
        workbook = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    except Exception:
        return None

    try:
        for worksheet in workbook.worksheets:
            rows = [list(row) for row in worksheet.iter_rows(values_only=True)]
            df = _build_snapshot_dataframe(rows)
            if df is not None:
                return df
    finally:
        workbook.close()
    return None


def _load_snapshot_csv(file_bytes: bytes) -> Optional[pd.DataFrame]:
    csv_df = _try_read_csv(file_bytes)
    if csv_df is None:
        return None

    if csv_df.empty:
        rows = [list(csv_df.columns)]
    else:
        normalized_df = csv_df.where(~csv_df.isna(), None)
        rows = [list(csv_df.columns)] + normalized_df.astype(object).values.tolist()
    return _build_snapshot_dataframe(rows)


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


def read_stock_snapshot(file_bytes: bytes, filename: str | None = None) -> Optional[pd.DataFrame]:
    """Прочитать отчёт «Остатки по складам» и вернуть подготовленный DataFrame."""

    if not file_bytes:
        return None

    df = _load_snapshot_excel(file_bytes)
    if df is None:
        df = _load_snapshot_csv(file_bytes)

    if df is None:
        return None

    df = _clean_dataframe(df)
    missing = [column for column in _SNAPSHOT_REQUIRED if column not in df.columns]
    if missing:
        return None

    return df[_SNAPSHOT_REQUIRED]
