import io
import logging
import os
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook
from zipfile import BadZipFile


logger = logging.getLogger(__name__)


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
_INTRANSIT_COLUMNS = ["Артикул продавца", "Артикул WB", "Склад", "Количество"]
_INTRANSIT_ALIASES = {
    "Артикул продавца": {
        "артикул продавца",
        "артикул поставщика",
        "артикул",
        "арт",
    },
    "Артикул WB": {
        "артикул wb",
        "арт wb",
        "код номенклатуры",
        "код товара",
        "артикул wildberries",
        "артикул wb.",
    },
    "Количество": {
        "количество, шт.",
        "количество",
        "кол-во",
        "количество шт",
        "qty",
        "шт",
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


def read_intransit_file(data: bytes, filename: str) -> pd.DataFrame:
    """Прочитать файл с поставками в пути и вернуть подготовленный DataFrame."""

    def _warehouse_name() -> str:
        base = os.path.splitext(os.path.basename(filename or ""))[0]
        return base.strip()

    def _prepare_dataframe(df: pd.DataFrame | None) -> pd.DataFrame | None:
        if df is None or df.empty:
            return None

        cleaned = df.dropna(how="all").dropna(axis=1, how="all")
        if cleaned.empty:
            return None

        cleaned.columns = [str(col).strip() for col in cleaned.columns]
        normalized = {_normalize_header_value(col): col for col in cleaned.columns}

        mapping: Dict[str, str] = {}
        for target, aliases in _INTRANSIT_ALIASES.items():
            for alias in aliases:
                column = normalized.get(alias)
                if column:
                    mapping[target] = column
                    break

        if "Количество" not in mapping:
            return None
        if "Артикул продавца" not in mapping and "Артикул WB" not in mapping:
            return None

        result = pd.DataFrame(index=cleaned.index)

        def _normalize_article(series: pd.Series) -> pd.Series:
            def _convert(value: object) -> Optional[str]:
                if pd.isna(value):
                    return None
                text = str(value).strip()
                if not text:
                    return None
                if text.lower() in {"nan", "none", "null"}:
                    return None
                return text or None

            return series.map(_convert)

        def _normalize_quantity(series: pd.Series) -> pd.Series:
            prepared = (
                series.astype(str)
                .str.replace("\xa0", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            numeric = pd.to_numeric(prepared, errors="coerce").fillna(0)
            return numeric.round().astype(int)

        seller_column = mapping.get("Артикул продавца")
        wb_column = mapping.get("Артикул WB")
        qty_column = mapping["Количество"]

        if seller_column:
            result["Артикул продавца"] = _normalize_article(cleaned[seller_column])
        else:
            result["Артикул продавца"] = pd.Series([None] * len(cleaned), index=cleaned.index)

        if wb_column:
            result["Артикул WB"] = _normalize_article(cleaned[wb_column])
        else:
            result["Артикул WB"] = pd.Series([None] * len(cleaned), index=cleaned.index)

        result["Количество"] = _normalize_quantity(cleaned[qty_column])
        result["Склад"] = _warehouse_name() or ""

        mask = (
            result["Количество"] > 0
        ) & (~result["Артикул продавца"].isna() | ~result["Артикул WB"].isna())

        filtered = result.loc[mask, _INTRANSIT_COLUMNS]
        filtered.attrs["intransit"] = True
        return filtered.reset_index(drop=True)

    # Пытаемся прочитать как Excel (шапка может быть на первой или второй строке)
    for header in (0, 1):
        try:
            excel_df = pd.read_excel(io.BytesIO(data), header=header, engine="openpyxl")
        except (BadZipFile, ValueError, TypeError):
            excel_df = None
        except Exception:
            continue
        prepared = _prepare_dataframe(excel_df)
        if prepared is not None:
            return prepared

    # Если Excel не подошёл, пробуем CSV с разными кодировками
    for header in (0, 1):
        for encoding in ("utf-8-sig", "utf-8", "cp1251", "utf-16"):
            try:
                csv_df = pd.read_csv(
                    io.BytesIO(data),
                    header=header,
                    encoding=encoding,
                    sep=None,
                    engine="python",
                )
            except Exception:
                continue
            prepared = _prepare_dataframe(csv_df)
            if prepared is not None:
                return prepared

    return pd.DataFrame(columns=_INTRANSIT_COLUMNS)


def read_fulfillment_stock_file(data: bytes, filename: str) -> pd.DataFrame:
    """
    Парсит файл остатков Фулфилмента.
    Ожидаемые колонки (любой из синонимов):
      - Артикул продавца: ["артикул продавца", "артикул поставщика", "артикул"]
      - Количество:       ["количество", "остаток", "кол-во", "шт"]
    Возвращает DataFrame с колонками ["Артикул продавца", "Количество"].
    """

    df = pd.DataFrame()

    # Пробуем прочитать как Excel с заголовками на первой или второй строке
    for header in (0, 1):
        try:
            df = pd.read_excel(io.BytesIO(data), header=header, engine="openpyxl")
            if not df.empty:
                break
        except (BadZipFile, ValueError):
            df = pd.DataFrame()
        except Exception:
            continue

    # Если Excel не прочитан, пробуем CSV с популярными кодировками
    if df.empty:
        for encoding in ("utf-16", "utf-8-sig", "utf-8", "cp1251"):
            try:
                df = pd.read_csv(
                    io.BytesIO(data),
                    header=0,
                    encoding=encoding,
                    sep=None,
                    engine="python",
                )
                if not df.empty:
                    break
            except Exception:
                continue

    if df.empty:
        return pd.DataFrame(columns=["Артикул продавца", "Количество"])

    normalized = {str(column).strip().lower(): column for column in df.columns}

    def _pick_column(options: Iterable[str]) -> Optional[str]:
        for option in options:
            if option in normalized:
                return normalized[option]
        return None

    seller_column = _pick_column(["артикул продавца", "артикул поставщика", "артикул"])
    quantity_column = _pick_column(["количество", "остаток", "кол-во", "шт"])

    if seller_column is None or quantity_column is None:
        return pd.DataFrame(columns=["Артикул продавца", "Количество"])

    result = pd.DataFrame()
    result["Артикул продавца"] = df[seller_column]
    result["Количество"] = (
        pd.to_numeric(
            df[quantity_column]
            .astype(str)
            .str.replace("\xa0", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False),
            errors="coerce",
        )
        .fillna(0)
        .astype(int)
    )

    mask = (~result["Артикул продавца"].isna()) & (result["Количество"] > 0)
    return result.loc[mask, ["Артикул продавца", "Количество"]].reset_index(drop=True)


def read_sku_reference(raw: bytes, filename: str) -> pd.DataFrame:
    """Читает справочник SKU и возвращает seller_sku / wb_sku / barcode.
    v2 + fallback: поддержка «Общие характеристики одним файлом…».
    """
    df = _read_sku_reference_v2(raw, filename) if "_read_sku_reference_v2" in globals() else pd.DataFrame()
    if df is None or df.empty:
        df = _read_sku_reference_fallback(raw, filename)
    return df


def _read_sku_reference_v2(raw: bytes, filename: str) -> pd.DataFrame:
    """
    Реальный справочник SKU:
      - лист «Товары»: Артикул продавца + Артикул WB
      - лист «Размеры и Баркоды»: Штрихкод + Артикул WB (либо seller)
    Возвращает столбцы: seller_sku / wb_sku / barcode.
    Если структура не похожа на справочник — возвращает пустой DataFrame.
    """
    import io
    import pandas as pd

    empty = pd.DataFrame(columns=["seller_sku", "wb_sku", "barcode"])
    if not raw:
        return empty
    try:
        excel = pd.ExcelFile(io.BytesIO(raw))
    except Exception:
        return empty
    if not excel.sheet_names:
        return empty

    # Наборы синонимов (нормализованные через _normalize_header_value)
    seller_opts = (
        "артикул продавца", "артикул поставщика", "seller sku", "sku продавца", "арт продавца"
    )
    wb_opts = (
        "артикул wb", "артикул wildberries", "wb артикул", "wb sku", "артикул вб", "код товара", "код номенклатуры"
    )
    barcode_opts = (
        "штрихкод", "штрих-код", "штрих код", "barcode", "баркод", "ean", "ean13", "шк товара"
    )
    # Признаки «не справочника» (инвентарные/логистические отчёты)
    not_ref_hints = ("склад", "количество", "остаток")

    def _pick(df: pd.DataFrame, options: tuple[str, ...]) -> Optional[str]:
        mapping = {_normalize_header_value(c): c for c in df.columns}
        for k in options:
            col = mapping.get(k)
            if col:
                return col
        return None

    def _looks_like_inventory(df: pd.DataFrame) -> bool:
        mapping = {_normalize_header_value(c): c for c in df.columns}
        return any(h in mapping for h in not_ref_hints)

    # 1) Ищем листы-кандидаты: seller+wb и (barcode & (wb|seller))
    seller_sheet: Optional[str] = None
    barcode_sheet: Optional[str] = None
    for sheet in excel.sheet_names:
        try:
            prev = _clean_dataframe(excel.parse(sheet, nrows=5))
        except Exception:
            continue
        if prev.empty:
            continue
        has_seller = _pick(prev, seller_opts) is not None
        has_wb = _pick(prev, wb_opts) is not None
        has_barcode = _pick(prev, barcode_opts) is not None
        if not _looks_like_inventory(prev) and has_seller and has_wb and seller_sheet is None:
            seller_sheet = sheet
        if has_barcode and (has_wb or has_seller) and barcode_sheet is None:
            barcode_sheet = sheet
        if seller_sheet and barcode_sheet:
            break

    if seller_sheet is None:
        return empty  # без пары seller+wb это точно не справочник

    # 2) Полностью читаем лист с seller+wb
    try:
        df_seller = _clean_dataframe(excel.parse(seller_sheet))
    except Exception:
        return empty
    if df_seller.empty:
        return empty

    seller_col = _pick(df_seller, seller_opts)
    wb_col = _pick(df_seller, wb_opts)
    if seller_col is None and wb_col is None:
        return empty

    def _norm(v) -> Optional[str]:
        if pd.isna(v):
            return None
        t = str(v).strip()
        return t or None

    base = pd.DataFrame()
    base["seller_sku"] = df_seller[seller_col].map(_norm) if seller_col else pd.Series([None]*len(df_seller))
    base["wb_sku"] = df_seller[wb_col].map(_norm) if wb_col else pd.Series([None]*len(df_seller))
    base = base.dropna(subset=["seller_sku", "wb_sku"], how="all").drop_duplicates()
    if base.empty:
        return empty

    # 3) Подтягиваем штрихкоды, если нашёлся соответствующий лист
    base["barcode"] = pd.Series([None]*len(base), index=base.index, dtype="object")
    if barcode_sheet:
        try:
            df_bar = _clean_dataframe(excel.parse(barcode_sheet))
        except Exception:
            df_bar = pd.DataFrame()
        if not df_bar.empty:
            bc_col = _pick(df_bar, barcode_opts)
            wb2_col = _pick(df_bar, wb_opts)
            seller2_col = _pick(df_bar, seller_opts)
            if bc_col and (wb2_col or seller2_col):
                bar = pd.DataFrame({"barcode": df_bar[bc_col].map(_norm)})
                if wb2_col:
                    bar["wb_sku"] = df_bar[wb2_col].map(_norm)
                    bar = bar.dropna(subset=["wb_sku", "barcode"]).drop_duplicates(subset=["wb_sku"])
                    base = base.merge(bar[["wb_sku", "barcode"]], on="wb_sku", how="left")
                elif seller2_col:
                    bar["seller_sku"] = df_bar[seller2_col].map(_norm)
                    bar = bar.dropna(subset=["seller_sku", "barcode"]).drop_duplicates(subset=["seller_sku"])
                    base = base.merge(bar[["seller_sku", "barcode"]], on="seller_sku", how="left")

    return base.loc[:, ["seller_sku", "wb_sku", "barcode"]].reset_index(drop=True)


def _read_sku_reference_fallback(raw: bytes, filename: str) -> pd.DataFrame:
    """
    Fallback‑парсер справочника SKU для книг с произвольными листами/заголовками.
    Ищет:
      • лист(ы) с парами: Артикул продавца (seller) и/или Артикул WB (wb);
      • лист(ы) со штрихкодами (barcode) плюс seller ИЛИ wb.
    Возвращает DataFrame со столбцами: seller_sku / wb_sku / barcode.
    Если структура не похожа на справочник — возвращает пустой DataFrame.
    """
    empty = pd.DataFrame(columns=["seller_sku", "wb_sku", "barcode"])
    if not raw:
        return empty
    try:
        xl = pd.ExcelFile(io.BytesIO(raw))
    except Exception:
        return empty
    if not xl.sheet_names:
        return empty

    seller_opts = (
        "артикул продавца",
        "артикул поставщика",
        "seller sku",
        "sku продавца",
        "арт продавца",
        "supplierarticle",
        "sellerarticle",
    )
    wb_opts = (
        "артикул wb",
        "артикул wildberries",
        "wb артикул",
        "wb sku",
        "артикул вб",
        "код товара",
        "код номенклатуры",
        "nmid",
        "nm id",
        "nmid товара",
    )
    barcode_opts = (
        "штрихкод",
        "штрих-код",
        "штрих код",
        "barcode",
        "баркод",
        "ean",
        "ean13",
        "шк товара",
    )

    def _pick(df: pd.DataFrame, options: tuple[str, ...]) -> Optional[str]:
        mapping = {_normalize_header_value(c): c for c in df.columns}
        for key in options:
            col = mapping.get(key)
            if col:
                return col
        return None

    def _norm(value: object) -> Optional[str]:
        if pd.isna(value):
            return None
        text = str(value).strip()
        if not text or text.lower() in {"none", "nan"}:
            return None
        text = text.replace("\xa0", "").replace(" ", "")
        if text.endswith(".0") and text[:-2].isdigit():
            text = text[:-2]
        return text or None

    seller_candidates: List[Tuple[str, Optional[str], Optional[str]]] = []
    barcode_candidates: List[Tuple[str, str, Optional[str]]] = []
    for sheet in xl.sheet_names:
        try:
            head = _clean_dataframe(xl.parse(sheet, nrows=50))
        except Exception:
            continue
        if head.empty:
            continue
        seller_col = _pick(head, seller_opts)
        wb_col = _pick(head, wb_opts)
        barcode_col = _pick(head, barcode_opts)
        if seller_col or wb_col:
            seller_candidates.append((sheet, seller_col, wb_col))
        if barcode_col and (wb_col or seller_col):
            barcode_candidates.append((sheet, barcode_col, wb_col or seller_col))

    if not seller_candidates:
        return empty

    sheet, seller_col, wb_col = seller_candidates[0]
    try:
        df_seller = _clean_dataframe(xl.parse(sheet))
    except Exception:
        return empty
    base = pd.DataFrame(index=df_seller.index)
    if seller_col:
        base["seller_sku"] = df_seller[seller_col].map(_norm)
    else:
        base["seller_sku"] = pd.Series([None] * len(df_seller), index=df_seller.index, dtype="object")
    if wb_col:
        base["wb_sku"] = df_seller[wb_col].map(_norm)
    else:
        base["wb_sku"] = pd.Series([None] * len(df_seller), index=df_seller.index, dtype="object")
    base = base.dropna(subset=["seller_sku", "wb_sku"], how="all").drop_duplicates()
    if base.empty:
        return empty

    base["barcode"] = pd.Series([None] * len(base), index=base.index, dtype="object")
    if barcode_candidates:
        sheet_bc, barcode_col, key_col = barcode_candidates[0]
        try:
            df_bar = _clean_dataframe(xl.parse(sheet_bc))
        except Exception:
            df_bar = pd.DataFrame()
        if not df_bar.empty and key_col and key_col in df_bar.columns:
            key_norm = df_bar[key_col].map(_norm)
            bar = pd.DataFrame({"barcode": df_bar[barcode_col].map(_norm)})
            bar["key"] = key_norm
            bar = bar.dropna(subset=["key", "barcode"]).drop_duplicates(subset=["key"])
            if base["wb_sku"].notna().any():
                base = base.merge(
                    bar.rename(columns={"key": "wb_sku"})[["wb_sku", "barcode"]],
                    on="wb_sku",
                    how="left",
                )
            else:
                base = base.merge(
                    bar.rename(columns={"key": "seller_sku"})[["seller_sku", "barcode"]],
                    on="seller_sku",
                    how="left",
                )

    return base.loc[:, ["seller_sku", "wb_sku", "barcode"]].reset_index(drop=True)
