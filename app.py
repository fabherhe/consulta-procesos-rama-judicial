import os
import subprocess
import sys
from datetime import datetime
from typing import Optional

import streamlit as st

from rama_scraper import (
    dataframe_to_excel_bytes,
    list_excel_sheets,
    process_dataframe,
    read_uploaded_dataframe,
)

st.set_page_config(page_title="Consulta Rama Judicial", page_icon="⚖️", layout="wide")


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
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def append_log(message: str, container, log_box):
    # We append each log line to the in-session UI log.
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.log_lines.append(f"[{timestamp}] {message}")
    container.caption(f"Último evento: {message}")
    log_box.code("\n".join(st.session_state.log_lines[-400:]), language="text")


def main():
    # We render the full Streamlit interface.
    init_state()
    st.markdown(
        """
        <style>
            .footer-note {
                position: fixed;
                right: 16px;
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
        <div class="footer-note">Esta app se crea para ahorrarle tiempo a mi amorsito 💕</div>
        """,
        unsafe_allow_html=True,
    )


    st.title("Consulta de radicados en Rama Judicial")
    st.write(
        "Sube un CSV o Excel, elige la columna del radicado y la app devolverá el mismo archivo "
        "con las columnas originales más las columnas `_rama`."
    )

    with st.expander("Notas rápidas", expanded=False):
        st.markdown(
            "- La app corre con Playwright en modo headless, así que no abre navegador visible.\n"
            "- Los logs se muestran en pantalla durante la ejecución y viven solo en la sesión actual.\n"
            "- Para archivos Excel con varias hojas, puedes elegir cuál procesar."
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
        compare_enabled = st.checkbox("Comparar contra columna de fecha de última actuación")

    compare_date_col = None
    if compare_enabled:
        compare_date_col = st.selectbox(
            "Columna de fecha base para comparar",
            input_df.columns.tolist(),
        )

    run_button = st.button("Ejecutar consulta", type="primary", width="stretch")

    if run_button:
        st.session_state.log_lines = []
        st.session_state.result_df = None
        st.session_state.result_bytes = None
        st.session_state.result_filename = None
        st.session_state.run_finished = False

        top_status = st.empty()
        progress_text = st.empty()
        progress_bar = st.progress(0)
        log_box = st.empty()

        with st.status("Preparando entorno...", expanded=True) as status:
            ok, message = ensure_chromium_installed()
            append_log("Validando instalación de Chromium para Playwright.", progress_text, log_box)

            if not ok:
                status.update(label="Falló la preparación del navegador", state="error")
                st.error(message)
                return

            append_log("Chromium listo para ejecutar consultas.", progress_text, log_box)
            top_status.info("Navegador headless preparado. Iniciando procesamiento...")

            def ui_log(msg: str):
                # We stream scraper logs into Streamlit in real time.
                append_log(msg, progress_text, log_box)

            def ui_progress(current: int, total: int, rad: str):
                # We update progress for each row being processed.
                ratio = current / total if total else 0
                progress_bar.progress(ratio)
                top_status.info(f"Procesando {current}/{total} radicados. Actual: {rad}")

            try:
                output_df = process_dataframe(
                    input_df,
                    radicado_col=radicado_col,
                    compare_date_col=compare_date_col,
                    log=ui_log,
                    progress=ui_progress,
                    headless=True,
                )
                output_bytes = dataframe_to_excel_bytes(output_df)
            except Exception as exc:
                status.update(label="La ejecución terminó con error", state="error")
                st.error(f"La ejecución falló: {exc}")
                return

            progress_bar.progress(1.0)
            status.update(label="Proceso terminado", state="complete")
            top_status.success("Consulta terminada. Ya puedes revisar la vista previa y descargar el archivo.")

        result_filename = f"resultado_rama_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        st.session_state.result_df = output_df
        st.session_state.result_bytes = output_bytes
        st.session_state.result_filename = result_filename
        st.session_state.run_finished = True

    if st.session_state.run_finished and st.session_state.result_df is not None:
        result_df = st.session_state.result_df

        st.subheader("Vista previa del resultado")
        st.dataframe(result_df.head(100), width="stretch")

        st.download_button(
            label="Descargar Excel resultado",
            data=st.session_state.result_bytes,
            file_name=st.session_state.result_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
        )

        if "status_rama" in result_df.columns:
            st.subheader("Resumen rápido")
            summary = result_df["status_rama"].value_counts(dropna=False).rename_axis("status_rama").reset_index(name="filas")
            st.dataframe(summary, width="stretch")


if __name__ == "__main__":
    main()
