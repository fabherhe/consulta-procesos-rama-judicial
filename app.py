import os
import subprocess
import sys
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from rama_scraper import (
    dataframe_to_excel_bytes,
    list_excel_sheets,
    process_dataframe,
    read_uploaded_dataframe,
)

st.set_page_config(page_title="Consulta Rama Judicial", page_icon="⚖️", layout="wide")

CHUNK_SIZE = 20


@st.cache_resource(show_spinner=False)
def ensure_chromium_installed() -> tuple[bool, str]:
    # We install Chromium once per app server when it is missing.
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/pw-browsers")

    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        return True, (result.stdout or "Chromium listo.").strip()

    message = (result.stderr or result.stdout or "No se pudo instalar Chromium.").strip()
    return False, message


def init_state():
    # We initialize all session keys used by the app.
    defaults = {
        "result_df": None,
        "result_bytes": None,
        "result_filename": None,
        "log_lines": [],
        "run_finished": False,
        "processing_paused": False,
        "pause_message": "",
        "resume_remaining_df": None,
        "resume_accumulated_df": None,
        "resume_radicado_col": None,
        "resume_compare_date_col": None,
        "resume_total_rows": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def append_log(message: str, container, log_box):
    # We append each log line to the in-session UI log.
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.log_lines.append(f"[{timestamp}] {message}")
    container.caption(f"Ultimo evento: {message}")
    log_box.code("\n".join(st.session_state.log_lines[-400:]), language="text")


def process_in_chunks(
    source_df: pd.DataFrame,
    radicado_col: str,
    compare_date_col: Optional[str],
    log,
    progress,
    headless: bool,
    absolute_offset: int,
    absolute_total: int,
):
    # We split long runs to preserve progress if a late chunk fails.
    outputs = []
    processed_local = 0
    error_text = ""

    for chunk_start in range(0, len(source_df), CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, len(source_df))
        chunk_df = source_df.iloc[chunk_start:chunk_end].copy()
        log(
            f"Procesando bloque {absolute_offset + chunk_start + 1}-"
            f"{absolute_offset + chunk_end} de {absolute_total}."
        )

        def chunk_progress(current: int, _total: int, rad: str, base: int = chunk_start):
            progress(absolute_offset + base + current, absolute_total, rad)

        try:
            chunk_out = process_dataframe(
                chunk_df,
                radicado_col=radicado_col,
                compare_date_col=compare_date_col,
                log=log,
                progress=chunk_progress,
                headless=headless,
            )
        except Exception as exc:
            error_text = str(exc)
            break

        outputs.append(chunk_out)
        processed_local = chunk_end

    if outputs:
        combined = pd.concat(outputs, ignore_index=True)
    else:
        combined = pd.DataFrame()

    return combined, processed_local, error_text


def run_processing_ui(
    source_df: pd.DataFrame,
    radicado_col: str,
    compare_date_col: Optional[str],
    absolute_offset: int,
    absolute_total: int,
):
    # We run one processing cycle (initial or continuation) with live UI feedback.
    top_status = st.empty()
    progress_text = st.empty()
    progress_bar = st.progress(0)
    log_box = st.empty()

    with st.status("Preparando entorno...", expanded=True) as status:
        ok, message = ensure_chromium_installed()
        append_log("Validando instalacion de Chromium para Playwright.", progress_text, log_box)

        if not ok:
            status.update(label="Fallo la preparacion del navegador", state="error")
            st.error(message)
            return pd.DataFrame(), 0, "No se pudo preparar Chromium"

        append_log("Chromium listo para ejecutar consultas.", progress_text, log_box)
        top_status.info("Navegador headless preparado. Iniciando procesamiento...")

        def ui_log(msg: str):
            append_log(msg, progress_text, log_box)

        def ui_progress(current: int, total: int, rad: str):
            ratio = current / total if total else 0
            progress_bar.progress(ratio)
            top_status.info(f"Procesando {current}/{total} radicados. Actual: {rad}")

        output_df, processed_local, error_text = process_in_chunks(
            source_df=source_df,
            radicado_col=radicado_col,
            compare_date_col=compare_date_col,
            log=ui_log,
            progress=ui_progress,
            headless=True,
            absolute_offset=absolute_offset,
            absolute_total=absolute_total,
        )

        if error_text:
            status.update(label="La ejecucion se detuvo", state="error")
        else:
            progress_bar.progress(1.0)
            status.update(label="Proceso terminado", state="complete")
            top_status.success("Consulta terminada. Ya puedes revisar la vista previa y descargar el archivo.")

    return output_df, processed_local, error_text


def main():
    # We render the full Streamlit interface.
    init_state()
    st.markdown(
        """
        <style>
            .footer-note {
                position: fixed;
                left: 16px;
                bottom: 10px;
                z-index: 9999;
                font-size: 0.85rem;
                color: #6b7280;
                background: rgba(255, 255, 255, 0.75);
                padding: 6px 10px;
                border-radius: 10px;
                backdrop-filter: blur(4px);
            }
        </style>
        <div class="footer-note">Esta app se crea para ahorrarle tiempo a mi amorsito</div>
        """,
        unsafe_allow_html=True,
    )

    st.title("Consulta de radicados en Rama Judicial")
    st.write(
        "Sube un CSV o Excel, elige la columna del radicado y la app devolvera el mismo archivo "
        "con las columnas originales mas las columnas `_rama`."
    )

    with st.expander("Notas rapidas", expanded=False):
        st.markdown(
            "- La app corre con Playwright en modo headless, asi que no abre navegador visible.\n"
            "- Los logs se muestran en pantalla durante la ejecucion y viven solo en la sesion actual.\n"
            "- Para archivos Excel con varias hojas, puedes elegir cual procesar."
        )

    uploaded_file = st.file_uploader("Archivo de entrada", type=["csv", "xlsx", "xls"])

    if not uploaded_file:
        return

    file_bytes = uploaded_file.getvalue()
    sheets = list_excel_sheets(file_bytes, uploaded_file.name)
    selected_sheet: Optional[str] = None

    if sheets:
        selected_sheet = st.selectbox("Hoja a procesar", sheets, index=0)

    try:
        input_df = read_uploaded_dataframe(file_bytes, uploaded_file.name, sheet_name=selected_sheet)
    except Exception as exc:
        st.error(f"No se pudo leer el archivo: {exc}")
        return

    if input_df.empty:
        st.warning("El archivo no tiene filas para procesar.")
        return

    st.subheader("Vista previa del archivo subido")
    st.dataframe(input_df.head(20), width="stretch")

    col1, col2 = st.columns(2)
    with col1:
        radicado_col = st.selectbox("Columna que contiene el radicado", input_df.columns.tolist())
    with col2:
        compare_enabled = st.checkbox("Comparar contra columna de fecha de ultima actuacion")

    compare_date_col = None
    if compare_enabled:
        compare_date_col = st.selectbox(
            "Columna de fecha base para comparar",
            input_df.columns.tolist(),
        )

    run_button = st.button("Ejecutar consulta", type="primary", width="stretch")

    continue_button = False
    if st.session_state.processing_paused and st.session_state.resume_remaining_df is not None:
        processed = len(st.session_state.resume_accumulated_df) if st.session_state.resume_accumulated_df is not None else 0
        total_rows = st.session_state.resume_total_rows
        st.warning(st.session_state.pause_message)
        st.caption(
            f"Solo pudimos extraer datos para los primeros {processed} de {total_rows} radicados. "
            "Puedes descargar el archivo parcial y continuar luego."
        )
        continue_button = st.button("Continuar consulta", width="stretch")

    if run_button:
        st.session_state.log_lines = []
        st.session_state.result_df = None
        st.session_state.result_bytes = None
        st.session_state.result_filename = None
        st.session_state.run_finished = False
        st.session_state.processing_paused = False
        st.session_state.pause_message = ""
        st.session_state.resume_remaining_df = None
        st.session_state.resume_accumulated_df = None
        st.session_state.resume_radicado_col = None
        st.session_state.resume_compare_date_col = None
        st.session_state.resume_total_rows = 0

        output_df, processed_local, error_text = run_processing_ui(
            source_df=input_df,
            radicado_col=radicado_col,
            compare_date_col=compare_date_col,
            absolute_offset=0,
            absolute_total=len(input_df),
        )

        if error_text:
            processed = len(output_df)
            remaining_df = input_df.iloc[processed_local:].copy()
            st.session_state.processing_paused = True
            st.session_state.pause_message = (
                "La consulta no esta retornando resultados de forma estable. "
                "Recomendamos descargar el avance y usar Continuar consulta."
            )
            st.session_state.resume_remaining_df = remaining_df
            st.session_state.resume_accumulated_df = output_df
            st.session_state.resume_radicado_col = radicado_col
            st.session_state.resume_compare_date_col = compare_date_col
            st.session_state.resume_total_rows = len(input_df)

            if not output_df.empty:
                st.session_state.result_df = output_df
                st.session_state.result_bytes = dataframe_to_excel_bytes(output_df)
                st.session_state.result_filename = (
                    f"resultado_rama_parcial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )

            if processed == 0 and "3 radicados validos consecutivos con 'Network Error' al inicio" in error_text:
                st.error("La consulta no esta retornando resultados. Por favor intenta mas tarde.")
            elif processed == 0:
                st.error(f"La ejecucion se detuvo antes de generar resultados: {error_text}")
        else:
            result_filename = f"resultado_rama_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            st.session_state.result_df = output_df
            st.session_state.result_bytes = dataframe_to_excel_bytes(output_df)
            st.session_state.result_filename = result_filename
            st.session_state.run_finished = True

    if continue_button and st.session_state.resume_remaining_df is not None:
        remaining_df = st.session_state.resume_remaining_df
        accumulated_df = st.session_state.resume_accumulated_df
        stored_radicado_col = st.session_state.resume_radicado_col
        stored_compare_date_col = st.session_state.resume_compare_date_col
        total_rows = st.session_state.resume_total_rows

        if accumulated_df is None:
            accumulated_df = pd.DataFrame()

        output_df, processed_local, error_text = run_processing_ui(
            source_df=remaining_df,
            radicado_col=stored_radicado_col,
            compare_date_col=stored_compare_date_col,
            absolute_offset=len(accumulated_df),
            absolute_total=total_rows,
        )

        if not output_df.empty:
            combined_df = pd.concat([accumulated_df, output_df], ignore_index=True)
        else:
            combined_df = accumulated_df

        if error_text:
            new_remaining = remaining_df.iloc[processed_local:].copy()
            st.session_state.processing_paused = True
            st.session_state.pause_message = (
                "La pagina parece inestable en este momento. "
                "Puedes descargar el avance actual y volver a continuar."
            )
            st.session_state.resume_remaining_df = new_remaining
            st.session_state.resume_accumulated_df = combined_df
            st.session_state.result_df = combined_df
            if not combined_df.empty:
                st.session_state.result_bytes = dataframe_to_excel_bytes(combined_df)
                st.session_state.result_filename = (
                    f"resultado_rama_parcial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )
        else:
            st.session_state.processing_paused = False
            st.session_state.pause_message = ""
            st.session_state.resume_remaining_df = None
            st.session_state.resume_accumulated_df = None
            st.session_state.resume_radicado_col = None
            st.session_state.resume_compare_date_col = None
            st.session_state.resume_total_rows = 0
            st.session_state.run_finished = True
            st.session_state.result_df = combined_df
            st.session_state.result_bytes = dataframe_to_excel_bytes(combined_df)
            st.session_state.result_filename = f"resultado_rama_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    if st.session_state.result_df is not None:
        result_df = st.session_state.result_df

        if st.session_state.processing_paused:
            st.subheader("Vista previa del resultado parcial")
        else:
            st.subheader("Vista previa del resultado")

        st.dataframe(result_df.head(100), width="stretch")

        if st.session_state.result_bytes is not None:
            st.download_button(
                label="Descargar Excel resultado",
                data=st.session_state.result_bytes,
                file_name=st.session_state.result_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )

        if "status_rama" in result_df.columns:
            st.subheader("Resumen rapido")
            summary = result_df["status_rama"].value_counts(dropna=False).rename_axis("status_rama").reset_index(name="filas")
            st.dataframe(summary, width="stretch")


if __name__ == "__main__":
    main()