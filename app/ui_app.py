from __future__ import annotations

from io import BytesIO

import pandas as pd
import streamlit as st

from engine.export_prototype import build_prototype_workbook
from engine.transform import (
    sales_by_warehouse_from_details,
    merge_sales_with_stock_today,
)
from wb_io.wb_readers import read_stock_history

st.set_page_config(page_title="Валидация парсинга отчетов WB", layout="wide")

st.title("Проверка парсинга отчетов WB")
st.caption(
    "Загрузите отчёты «История остатков», чтобы построить валидационный Excel «Input_Prototype_Filled.xlsx»."
)

uploaded_files = st.file_uploader(
    label="Загрузите отчёты",
    type=["xlsx", "xls", "csv"],
    accept_multiple_files=True,
)

if "validation_bytes" not in st.session_state:
    st.session_state["validation_bytes"] = None
if "validation_log" not in st.session_state:
    st.session_state["validation_log"] = ""


_SALES_STOCK_COLUMNS = [
    "Артикул продавца",
    "Артикул WB",
    "Склад",
    "Заказали, шт",
    "Остаток на сегодня",
]


def _process_files(files: list[BytesIO], names: list[str]) -> tuple[BytesIO | None, str]:
    logs: list[str] = []
    combined_frames: list[pd.DataFrame] = []

    for file_bytes, name in zip(files, names):
        sheets = read_stock_history(file_bytes.getvalue(), name)
        if not sheets:
            logs.append(f"Файл {name}: не удалось определить нужные листы")
            continue

        detected = ", ".join(sheets.keys())
        logs.append(f"Файл {name}: распознаны листы — {detected}")

        detail_sheet = next((df for title, df in sheets.items() if title.lower() == "детальная информация"), None)
        daily_sheet = next((df for title, df in sheets.items() if title.lower() == "остатки по дням"), None)

        sales_df = sales_by_warehouse_from_details(detail_sheet) if detail_sheet is not None else pd.DataFrame()
        merged = merge_sales_with_stock_today(sales_df, detail_sheet, daily_sheet)
        if not merged.empty:
            combined_frames.append(merged)

    if combined_frames:
        sales_stock_result = pd.concat(combined_frames, ignore_index=True).reindex(columns=_SALES_STOCK_COLUMNS)
    else:
        sales_stock_result = pd.DataFrame(columns=_SALES_STOCK_COLUMNS)

    logs.append(
        "Итог: лист «Продажи и остатки по складам» — {rows} строк.".format(
            rows=len(sales_stock_result),
        )
    )

    workbook = build_prototype_workbook(sales_stock_result)
    return workbook, "\n".join(logs)


if st.button("Собрать валидационный инпут"):
    if not uploaded_files:
        st.warning("Сначала загрузите хотя бы один файл отчёта.")
    else:
        file_buffers = []
        file_names = []
        for uploaded in uploaded_files:
            data = uploaded.read()
            file_buffers.append(BytesIO(data))
            file_names.append(uploaded.name)
        workbook, log_text = _process_files(file_buffers, file_names)
        st.session_state["validation_bytes"] = workbook.getvalue() if workbook else None
        st.session_state["validation_log"] = log_text
        if workbook is None:
            st.error("Не удалось подготовить данные. Проверьте загруженные отчёты и попробуйте снова.")
        else:
            st.success("Валидационный Excel собран. Ниже доступен лог и кнопка для скачивания.")

if st.session_state["validation_log"]:
    st.subheader("Лог обработки")
    st.text(st.session_state["validation_log"])

if st.session_state["validation_bytes"]:
    st.download_button(
        label="Скачать Excel",
        data=st.session_state["validation_bytes"],
        file_name="Input_Prototype_Filled.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
