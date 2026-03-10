import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

CHUNK_SIZE = 20
CHECKPOINT_DIR = Path(tempfile.gettempdir()) / "rama_streamlit_checkpoints"
BLOCKED_BOOT_MESSAGE = "3 radicados validos consecutivos con 'Network Error' al inicio"

st.set_page_config(page_title="Consulta Rama Judicial", page_icon="R", layout="wide")


def get_scraper_module():
    # We import scraper lazily and retry once if import cache gets inconsistent.
    import importlib

    module_name = "rama_scraper"
    last_error = None

    for attempt in range(1, 4):
        try:
            if attempt > 1:
                importlib.invalidate_caches()
                sys.modules.pop(module_name, None)
            return importlib.import_module(module_name)
        except KeyError as exc:
            last_error = exc
            sys.modules.pop(module_name, None)
        except Exception as exc:
            last_error = exc
            break

    raise RuntimeError(
        "No fue posible cargar el modulo rama_scraper. "
        f"Ultimo error: {type(last_error).__name__}: {last_error}"
    )


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
    container.caption(f"Ultimo evento: {message}")
    log_box.code("\n".join(st.session_state.log_lines[-400:]), language="text")


def build_run_key(
    file_bytes: bytes,
    filename: str,
    selected_sheet: Optional[str],
    radicado_col: str,
    compare_date_col: Optional[str],
) -> str:
    # We generate a deterministic key for persisted progress for this exact run configuration.
    hasher = hashlib.sha256()
    hasher.update(file_bytes)
    hasher.update(filename.encode("utf-8", errors="ignore"))
    hasher.update((selected_sheet or "").encode("utf-8", errors="ignore"))
    hasher.update(radicado_col.encode("utf-8", errors="ignore"))
    hasher.update((compare_date_col or "").encode("utf-8", errors="ignore"))
    return hasher.hexdigest()[:24]


def checkpoint_paths(run_key: str) -> tuple[Path, Path]:
    # We map a run key to its dataframe and metadata files.
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    data_path = CHECKPOINT_DIR / f"{run_key}.pkl"
    meta_path = CHECKPOINT_DIR / f"{run_key}.json"
    return data_path, meta_path


def save_checkpoint(
    run_key: str,
    result_df: pd.DataFrame,
    processed_rows: int,
    total_rows: int,
    status: str,
    error_text: str,
):
    # We persist progress so users can continue after crashes or app restarts.
    data_path, meta_path = checkpoint_paths(run_key)

    result_df.to_pickle(data_path)
    meta = {
        "processed_rows": int(processed_rows),
        "total_rows": int(total_rows),
        "status": status,
        "error_text": error_text,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=True), encoding="utf-8")


def load_checkpoint(run_key: str) -> tuple[Optional[pd.DataFrame], Optional[dict]]:
    # We recover checkpointed progress if it exists and is readable.
    data_path, meta_path = checkpoint_paths(run_key)
    if not data_path.exists() or not meta_path.exists():
        return None, None

    try:
        result_df = pd.read_pickle(data_path)
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return result_df, meta
    except Exception:
        clear_checkpoint(run_key)
        return None, None


def clear_checkpoint(run_key: str):
    # We remove persisted progress for a run key.
    data_path, meta_path = checkpoint_paths(run_key)
    for path in [data_path, meta_path]:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def run_processing_ui(
    scraper,
    source_df: pd.DataFrame,
    radicado_col: str,
    compare_date_col: Optional[str],
    absolute_offset: int,
    absolute_total: int,
    run_key: str,
    base_df: Optional[pd.DataFrame],
):
    # We process in chunks and checkpoint after each completed block.
    top_status = st.empty()
    progress_text = st.empty()
    progress_bar = st.progress(0)
    log_box = st.empty()

    accumulated_df = base_df.copy() if base_df is not None else pd.DataFrame()
    processed_local = 0
    error_text = ""

    with st.status("Preparando entorno...", expanded=True) as status:
        ok, message = ensure_chromium_installed()
        append_log("Validando instalacion de Chromium para Playwright.", progress_text, log_box)

        if not ok:
            status.update(label="Fallo la preparacion del navegador", state="error")
            st.error(message)
            return accumulated_df, processed_local, "No se pudo preparar Chromium"

        append_log("Chromium listo para ejecutar consultas.", progress_text, log_box)
        top_status.info("Navegador headless preparado. Iniciando procesamiento...")

        def ui_log(msg: str):
            append_log(msg, progress_text, log_box)

        def ui_progress(current: int, total: int, rad: str):
            ratio = current / total if total else 0
            progress_bar.progress(ratio)
            top_status.info(f"Procesando {current}/{total} radicados. Actual: {rad}")

        for chunk_start in range(0, len(source_df), CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, len(source_df))
            chunk_df = source_df.iloc[chunk_start:chunk_end].copy()
            ui_log(
                f"Procesando bloque {absolute_offset + chunk_start + 1}-"
                f"{absolute_offset + chunk_end} de {absolute_total}."
            )

            def chunk_progress(current: int, _chunk_total: int, rad: str, base: int = chunk_start):
                ui_progress(absolute_offset + base + current, absolute_total, rad)

            try:
                chunk_out = scraper.process_dataframe(
                    chunk_df,
                    radicado_col=radicado_col,
                    compare_date_col=compare_date_col,
                    log=ui_log,
                    progress=chunk_progress,
                    headless=True,
                )
            except Exception as exc:
                error_text = str(exc)
                break

            if accumulated_df.empty:
                accumulated_df = chunk_out
            else:
                accumulated_df = pd.concat([accumulated_df, chunk_out], ignore_index=True)

            processed_local = chunk_end
            save_checkpoint(
                run_key=run_key,
                result_df=accumulated_df,
                processed_rows=absolute_offset + processed_local,
                total_rows=absolute_total,
                status="running",
                error_text="",
            )

        if error_text:
            status.update(label="La ejecucion se detuvo", state="error")
            top_status.warning("La pagina parece inestable. Puedes descargar progreso y continuar.")
            save_checkpoint(
                run_key=run_key,
                result_df=accumulated_df,
                processed_rows=absolute_offset + processed_local,
                total_rows=absolute_total,
                status="paused",
                error_text=error_text,
            )
        else:
            progress_bar.progress(1.0)
            status.update(label="Proceso terminado", state="complete")
            top_status.success("Consulta terminada. Ya puedes revisar y descargar el archivo.")
            save_checkpoint(
                run_key=run_key,
                result_df=accumulated_df,
                processed_rows=absolute_total,
                total_rows=absolute_total,
                status="completed",
                error_text="",
            )

    return accumulated_df, processed_local, error_text


def main():
    # We render the full Streamlit interface.
    init_state()

    try:
        scraper = get_scraper_module()
    except Exception as exc:
        st.error("No se pudo cargar el motor de consulta.")
        st.caption(
            "La app se recupero de un reinicio, pero fallo al importar modulo interno. "
            "Reintenta en unos segundos o reinicia la app en Streamlit Cloud."
        )
        st.code(str(exc))
        return

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
            "- El progreso se guarda por bloques para poder continuar si la sesion se reinicia."
        )

    uploaded_file = st.file_uploader("Archivo de entrada", type=["csv", "xlsx", "xls"])
    if not uploaded_file:
        return

    file_bytes = uploaded_file.getvalue()
    sheets = scraper.list_excel_sheets(file_bytes, uploaded_file.name)
    selected_sheet: Optional[str] = None
    if sheets:
        selected_sheet = st.selectbox("Hoja a procesar", sheets, index=0)

    try:
        input_df = scraper.read_uploaded_dataframe(file_bytes, uploaded_file.name, sheet_name=selected_sheet)
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
        compare_date_col = st.selectbox("Columna de fecha base para comparar", input_df.columns.tolist())

    run_key = build_run_key(
        file_bytes=file_bytes,
        filename=uploaded_file.name,
        selected_sheet=selected_sheet,
        radicado_col=radicado_col,
        compare_date_col=compare_date_col,
    )

    saved_df, saved_meta = load_checkpoint(run_key)
    saved_processed = int(saved_meta.get("processed_rows", 0)) if saved_meta else 0
    total_rows = len(input_df)

    run_button = st.button("Ejecutar consulta", type="primary", width="stretch")

    continue_button = False
    discard_button = False
    if saved_df is not None and 0 < saved_processed < total_rows:
        st.warning(
            f"Solo pudimos extraer datos para los primeros {saved_processed} numeros de radicado. "
            "La pagina parece estar inestable, recomendamos guardar el archivo con el progreso "
            "y oprimir Continuar consulta para terminar el trabajo."
        )
        if saved_meta and saved_meta.get("error_text"):
            st.caption(f"Detalle tecnico de la ultima pausa: {saved_meta['error_text']}")
        continue_button = st.button("Continuar consulta", width="stretch")
        discard_button = st.button("Descartar progreso guardado", width="stretch")

    if discard_button:
        clear_checkpoint(run_key)
        st.session_state.result_df = None
        st.session_state.result_bytes = None
        st.session_state.result_filename = None
        st.session_state.run_finished = False
        st.rerun()

    if run_button:
        st.session_state.log_lines = []
        st.session_state.result_df = None
        st.session_state.result_bytes = None
        st.session_state.result_filename = None
        st.session_state.run_finished = False
        clear_checkpoint(run_key)

        output_df, _, error_text = run_processing_ui(
            scraper=scraper,
            source_df=input_df,
            radicado_col=radicado_col,
            compare_date_col=compare_date_col,
            absolute_offset=0,
            absolute_total=total_rows,
            run_key=run_key,
            base_df=None,
        )

        if error_text:
            if output_df.empty and BLOCKED_BOOT_MESSAGE in error_text:
                st.error("La consulta no esta retornando resultados. Por favor intenta mas tarde.")
            elif output_df.empty:
                st.error(f"La ejecucion se detuvo antes de generar resultados: {error_text}")
        else:
            st.session_state.run_finished = True

    if continue_button and saved_df is not None:
        remaining_df = input_df.iloc[saved_processed:].copy()

        output_df, _, error_text = run_processing_ui(
            scraper=scraper,
            source_df=remaining_df,
            radicado_col=radicado_col,
            compare_date_col=compare_date_col,
            absolute_offset=saved_processed,
            absolute_total=total_rows,
            run_key=run_key,
            base_df=saved_df,
        )

        if error_text and output_df.empty and BLOCKED_BOOT_MESSAGE in error_text:
            st.error("La consulta no esta retornando resultados. Por favor intenta mas tarde.")
        elif not error_text:
            st.session_state.run_finished = True

    # We reload the checkpoint after actions to always show the latest persisted state.
    display_df, display_meta = load_checkpoint(run_key)
    if display_df is not None:
        processed_rows = int(display_meta.get("processed_rows", len(display_df))) if display_meta else len(display_df)
        if display_df.empty and processed_rows == 0:
            return
        is_partial = processed_rows < total_rows

        if is_partial:
            st.subheader("Vista previa del resultado parcial")
            filename = f"resultado_rama_parcial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            st.subheader("Vista previa del resultado")
            filename = f"resultado_rama_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        st.dataframe(display_df.head(100), width="stretch")

        result_bytes = scraper.dataframe_to_excel_bytes(display_df)
        st.download_button(
            label="Descargar Excel resultado",
            data=result_bytes,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
        )

        if "status_rama" in display_df.columns:
            st.subheader("Resumen rapido")
            summary = (
                display_df["status_rama"]
                .value_counts(dropna=False)
                .rename_axis("status_rama")
                .reset_index(name="filas")
            )
            st.dataframe(summary, width="stretch")


if __name__ == "__main__":
    main()
