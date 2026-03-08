import io
import os
import random
import re
import time
import unicodedata
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

URL = "https://consultaprocesos.ramajudicial.gov.co/Procesos/NumeroRadicacion"

MAX_SEARCH_RETRIES = 3
MAX_DETAIL_OPEN_RETRIES = 2
MAX_DETAIL_DATA_RETRIES = 3
MAX_ACTUACIONES_RETRIES = 3
MAX_CONSECUTIVE_SEARCH_NETWORK_FAILURES = 3

DATE_YMD_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
DATE_MDY_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

SUBJECT_FIELD_MAP = {
    "demandante": "demandante_rama",
    "demandado": "demandado_rama",
    "ministerio publico": "ministerio_publico_rama",
    "autoridad de conocimiento": "autoridad_conocimiento_rama",
    "llamamiento en garantia": "llamamiento_garantia_rama",
    "sin tipo de sujeto": "sin_tipo_sujeto_rama",
}

DETAIL_FIELD_MAP = {
    "fecha de consulta": "fecha_consulta_rama",
    "fecha de replicacion de datos": "fecha_replicacion_datos_rama",
    "fecha de radicacion": "fecha_radicacion_rama",
    "despacho": "despacho_rama",
    "ponente": "ponente_rama",
    "tipo de proceso": "tipo_de_proceso_rama",
    "clase de proceso": "clase_de_proceso_rama",
    "subclase de proceso": "subclase_de_proceso_rama",
    "recurso": "recurso_rama",
    "ubicacion del expediente": "ubicacion_expediente_rama",
    "contenido de radicacion": "contenido_radicacion_rama",
}

DETAIL_SECTION_HEADERS = {
    "detalle del proceso",
    "datos del proceso",
    "sujetos procesales",
    "documentos del proceso",
    "actuaciones",
    "descargar doc",
    "descargar csv",
    "regresar al listado",
}

LogFn = Callable[[str], None]
ProgressFn = Callable[[int, int, str], None]


def human_pause(a: int = 900, b: int = 2200):
    # We sleep a random amount of time to mimic human pacing.
    time.sleep(random.uniform(a / 1000, b / 1000))


def safe_str(x) -> str:
    # We normalize None and NaN-like values to empty strings.
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def normalize_spaces(text: str) -> str:
    # We collapse repeated whitespace into a single space.
    return re.sub(r"\s+", " ", safe_str(text)).strip()


def strip_accents(text: str) -> str:
    # We remove diacritics to make label matching robust.
    normalized = unicodedata.normalize("NFKD", safe_str(text))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def canonical_text(text: str) -> str:
    # We normalize labels and other comparisons into a stable format.
    value = strip_accents(text)
    value = value.replace(":", " ")
    value = normalize_spaces(value).lower()
    return value


def is_valid_radicado(rad: str) -> bool:
    # We accept only 23 numeric digits.
    value = safe_str(rad)
    return value.isdigit() and len(value) == 23


def locator_is_visible(locator) -> bool:
    # We safely check whether a locator exists and is visible.
    try:
        return locator.count() > 0 and locator.first.is_visible()
    except Exception:
        return False


def click_first_visible(locators) -> bool:
    # We click the first visible locator from a list of candidates.
    for locator in locators:
        try:
            if locator_is_visible(locator):
                locator.first.click()
                return True
        except Exception:
            pass
    return False


def wait_for_network_idle_soft(page, timeout: int = 12000):
    # We wait softly for network idle because this site can be unstable.
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PlaywrightTimeoutError:
        pass


def extract_first_date_token(value: str) -> str:
    # We extract the first recognizable date token from a string.
    raw = safe_str(value)

    match_ymd = DATE_YMD_RE.search(raw)
    if match_ymd:
        return match_ymd.group(0)

    match_mdy = DATE_MDY_RE.search(raw)
    if match_mdy:
        return match_mdy.group(0)

    return ""


def parse_date_any(value: str):
    # We parse supported date formats into a date object.
    token = extract_first_date_token(value)
    if not token:
        return None

    patterns = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y/%m/%d",
    ]

    for pattern in patterns:
        try:
            return datetime.strptime(token, pattern).date()
        except ValueError:
            pass

    return None


def normalize_date_value(value: str) -> str:
    # We normalize output dates into YYYY-MM-DD when possible.
    parsed = parse_date_any(value)
    if parsed is None:
        return safe_str(value)
    return parsed.isoformat()


def compare_dates_flag(base_date: str, rama_date: str) -> bool:
    # We compare normalized dates from the base file and Rama output.
    left = parse_date_any(base_date)
    right = parse_date_any(rama_date)

    if left is None or right is None:
        return False

    return left == right


def difference_dates_days(base_date: str, rama_date: str):
    # We return the day difference between the base file and Rama output.
    left = parse_date_any(base_date)
    right = parse_date_any(rama_date)

    if left is None or right is None:
        return ""

    return (right - left).days


def parse_summary_dates(text: str) -> dict:
    # We extract filing date and latest action date from the results cell.
    raw = safe_str(text)
    matches = DATE_YMD_RE.findall(raw)

    filing_date = matches[0] if len(matches) >= 1 else ""
    latest_date = matches[-1] if len(matches) >= 1 else ""

    return {
        "fecha_radicacion_resumen_rama": filing_date,
        "fecha_ultima_actuacion_resumen_rama": latest_date,
    }


def parse_latest_date_from_results_cell(text: str) -> datetime:
    # We extract all YYYY-MM-DD dates and keep the most recent one.
    raw = safe_str(text)
    matches = DATE_YMD_RE.findall(raw)

    parsed_dates = []
    for value in matches:
        try:
            parsed_dates.append(datetime.strptime(value, "%Y-%m-%d"))
        except ValueError:
            pass

    return max(parsed_dates) if parsed_dates else datetime.min


def parse_multivalue_labeled_text(text: str, field_map: dict) -> dict:
    # We parse multiline labeled text where a label can appear multiple times.
    buckets = {out_key: [] for out_key in set(field_map.values())}
    current_field = None

    lines = [normalize_spaces(line) for line in safe_str(text).splitlines()]
    lines = [line for line in lines if line]

    for line in lines:
        matched_label = False

        if ":" in line:
            left, right = line.split(":", 1)
            canon_label = canonical_text(left)

            if canon_label in field_map:
                current_field = field_map[canon_label]
                value = normalize_spaces(right).strip(" -")

                if value:
                    buckets[current_field].append(value)
                else:
                    buckets[current_field].append("")

                matched_label = True
            else:
                current_field = None

        if matched_label:
            continue

        canon_line = canonical_text(line.rstrip(":"))
        if canon_line in field_map:
            current_field = field_map[canon_line]
            buckets[current_field].append("")
            continue

        if current_field and buckets[current_field]:
            buckets[current_field][-1] = normalize_spaces(
                f"{buckets[current_field][-1]} {line}"
            ).strip(" -")
        elif current_field:
            buckets[current_field].append(normalize_spaces(line).strip(" -"))

    result = {}
    for out_key, values in buckets.items():
        cleaned = []
        for value in values:
            normalized = normalize_spaces(value).strip(" -")
            if normalized:
                cleaned.append(normalized)

        result[out_key] = " | ".join(dict.fromkeys(cleaned))

    return result


def parse_subjects_text(text: str) -> dict:
    # We parse the 'Sujetos Procesales' block into separate subject columns.
    result = parse_multivalue_labeled_text(text, SUBJECT_FIELD_MAP)
    result["sujetos_procesales_raw_rama"] = safe_str(text)
    return result


def extract_summary_row_data(row) -> dict:
    # We extract relevant data from the selected row in the search results table.
    cells = row.locator("td")
    n = cells.count()

    date_cell = safe_str(cells.nth(2).inner_text()) if n > 2 else ""
    despacho_cell = safe_str(cells.nth(3).inner_text()) if n > 3 else ""
    subjects_cell = safe_str(cells.nth(4).inner_text()) if n > 4 else ""

    date_parts = parse_summary_dates(date_cell)
    subjects = parse_subjects_text(subjects_cell)

    result = {
        "despacho_departamento_resumen_rama": normalize_spaces(despacho_cell),
        "fecha_radicacion_resumen_rama": date_parts["fecha_radicacion_resumen_rama"],
        "fecha_ultima_actuacion_resumen_rama": date_parts["fecha_ultima_actuacion_resumen_rama"],
    }
    result.update(subjects)
    return result


def detect_popup_kind(page):
    # We detect known popup states using loose fragments instead of exact full sentences.
    popup_patterns = {
        "multiple_records": [
            "varios registros",
            "mismo número de Radicación",
            "consulte todos sus procesos",
        ],
        "network_error": [
            "Error: Network Error",
            "Network Error",
        ],
    }

    for kind, fragments in popup_patterns.items():
        for fragment in fragments:
            locator = page.get_by_text(fragment, exact=False)
            if locator_is_visible(locator):
                return kind

    return None


def detect_no_actuaciones_message(page) -> bool:
    # We detect the empty-state message for actuaciones.
    candidates = [
        page.get_by_text("El registro no posee actuaciones registradas", exact=False),
        page.get_by_text("no posee actuaciones registradas", exact=False),
    ]

    for locator in candidates:
        if locator_is_visible(locator):
            return True

    return False


def close_popup_with_back(page) -> bool:
    # We try several back or close button variants because the popup structure may vary.
    candidates = [
        page.get_by_role("button", name=re.compile(r"^\s*VOLVER\s*$", re.I)),
        page.get_by_text("VOLVER", exact=False),
        page.get_by_role("button", name=re.compile(r"^\s*CERRAR\s*$", re.I)),
        page.get_by_text("CERRAR", exact=False),
        page.get_by_role("button", name=re.compile(r"^\s*ACEPTAR\s*$", re.I)),
        page.get_by_text("ACEPTAR", exact=False),
    ]

    if click_first_visible(candidates):
        human_pause(600, 1200)
        return True

    return False


def reset_to_search(page):
    # We always reset to the search page to start from a clean state.
    page.goto(URL, wait_until="domcontentloaded")
    human_pause()


def prepare_search(page, rad: str):
    # We prepare the search form from scratch.
    todos_procesos = page.get_by_text("Todos los Procesos", exact=False)
    if locator_is_visible(todos_procesos):
        todos_procesos.click()
        human_pause(300, 900)

    input_rad = page.get_by_placeholder("Ingrese los 23 dígitos del número de Radicación")
    input_rad.fill(rad)
    human_pause(300, 900)


def wait_for_search_outcome(page, timeout_ms: int = 20000) -> str:
    # We wait until the search resolves into results, popup, or timeout.
    deadline = time.time() + (timeout_ms / 1000)
    results_table = page.locator("table").filter(has_text="Número de Radicación").first

    while time.time() < deadline:
        popup_kind = detect_popup_kind(page)
        if popup_kind:
            return popup_kind

        try:
            if results_table.is_visible():
                return "results"
        except Exception:
            pass

        time.sleep(0.25)

    return "timeout"


def search_with_retries(page, rad: str, log: LogFn, max_retries: int = 3):
    # We search a radicado and retry if we hit transient network errors.
    multiple_records_found = False
    network_error_retries = 0
    last_error = ""

    for attempt in range(1, max_retries + 1):
        log(f"    -> Intento de consulta {attempt}/{max_retries}")

        reset_to_search(page)
        prepare_search(page, rad)

        page.get_by_role("button", name="CONSULTAR").click()

        wait_for_network_idle_soft(page)
        human_pause(700, 1400)

        outcome = wait_for_search_outcome(page, timeout_ms=18000)

        if outcome == "results":
            log("    -> Tabla de resultados visible.")
            return {
                "popup_multiple_records_rama": multiple_records_found,
                "network_error_retries_rama": network_error_retries,
            }

        if outcome == "multiple_records":
            log("    -> Popup de múltiples registros detectado.")
            multiple_records_found = True
            close_popup_with_back(page)

            table = page.locator("table").filter(has_text="Número de Radicación").first
            table.wait_for(state="visible", timeout=12000)

            log("    -> Popup cerrado y tabla disponible.")
            return {
                "popup_multiple_records_rama": multiple_records_found,
                "network_error_retries_rama": network_error_retries,
            }

        if outcome == "network_error":
            log("    -> Network Error detectado. Se reintentará.")
            network_error_retries += 1
            last_error = "Network Error"
            close_popup_with_back(page)
            human_pause(1200, 2200)
            continue

        log("    -> La búsqueda no resolvió en un estado conocido. Se reintentará.")
        last_error = f"Estado inesperado: {outcome}"
        close_popup_with_back(page)
        human_pause(1200, 2200)

    if network_error_retries >= max_retries and last_error == "Network Error":
        raise RuntimeError(
            "NETWORK_ERROR_PERSISTENT: "
            f"La Rama devolvió 'Network Error' en los {max_retries} intentos del radicado {rad}."
        )

    raise RuntimeError(
        f"No fue posible consultar el radicado {rad} después de {max_retries} intentos. "
        f"Último error: {last_error}"
    )


def choose_best_result_row(page, rad: str, log: LogFn):
    # We inspect all result rows and choose the one with the most recent latest-action date.
    table = page.locator("table").filter(has_text="Número de Radicación").first
    table.wait_for(state="visible", timeout=15000)

    rows = table.locator("tbody tr")
    row_count = rows.count()

    if row_count == 0:
        raise RuntimeError(f"No se encontraron filas de resultados para el radicado {rad}.")

    best_row = None
    best_row_index = -1
    best_date = datetime.min
    best_summary = {}

    for i in range(row_count):
        row = rows.nth(i)
        row_text = safe_str(row.inner_text())

        if rad not in row_text:
            continue

        cells = row.locator("td")
        n = cells.count()
        date_cell_text = safe_str(cells.nth(2).inner_text()) if n > 2 else row_text
        latest_date = parse_latest_date_from_results_cell(date_cell_text)

        log(
            f"    -> Fila {i + 1}: "
            f"{latest_date.strftime('%Y-%m-%d') if latest_date != datetime.min else 'sin fecha válida'}"
        )

        if best_row is None or latest_date > best_date:
            best_row = row
            best_row_index = i + 1
            best_date = latest_date
            best_summary = extract_summary_row_data(row)

    if best_row is None:
        raise RuntimeError(
            f"Se encontraron resultados para {rad}, pero no se pudo identificar la fila correcta."
        )

    log(
        "    -> Se abrirá la fila con última actuación más reciente: "
        f"{best_date.strftime('%Y-%m-%d') if best_date != datetime.min else 'sin fecha válida'}"
    )

    return best_row, best_row_index, best_date, best_summary


def click_result_row(row, rad: str):
    # We try several selectors to open the chosen result row.
    candidates = [
        row.get_by_role("link", name=rad),
        row.locator("a").filter(has_text=rad),
        row.get_by_text(rad, exact=False),
    ]

    if not click_first_visible(candidates):
        raise RuntimeError(f"No fue posible abrir la fila del radicado {rad}.")


def wait_for_detail_outcome(page, timeout_ms: int = 15000) -> str:
    # We wait for the detail screen or a popup after clicking the result row.
    deadline = time.time() + (timeout_ms / 1000)

    while time.time() < deadline:
        popup_kind = detect_popup_kind(page)
        if popup_kind == "network_error":
            return "network_error"

        candidates = [
            page.get_by_text("DETALLE DEL PROCESO", exact=False),
            page.get_by_text("Fecha de Radicación:", exact=False),
            page.get_by_text("DATOS DEL PROCESO", exact=False),
            page.get_by_text("ACTUACIONES", exact=False),
        ]

        for locator in candidates:
            if locator_is_visible(locator):
                return "detail"

        time.sleep(0.25)

    return "timeout"


def open_detail_with_retries(page, rad: str, log: LogFn, max_retries: int = 2):
    # We open the selected detail page and retry if we hit a transient popup.
    last_error = ""

    for attempt in range(1, max_retries + 1):
        log(f"    -> Intento de abrir detalle {attempt}/{max_retries}")

        best_row, best_row_index, best_date, best_summary = choose_best_result_row(page, rad, log)

        human_pause()
        click_result_row(best_row, rad)

        wait_for_network_idle_soft(page)
        human_pause(700, 1400)

        outcome = wait_for_detail_outcome(page, timeout_ms=15000)

        if outcome == "detail":
            result = {
                "selected_result_row_index_rama": best_row_index,
                "selected_result_latest_date_rama": (
                    best_date.strftime("%Y-%m-%d") if best_date != datetime.min else ""
                ),
            }
            result.update(best_summary)
            return result

        if outcome == "network_error":
            log("    -> Network Error al abrir detalle. Se reintentará.")
            close_popup_with_back(page)
            human_pause(1000, 1800)
            last_error = "Network Error al abrir detalle"
            continue

        last_error = f"Timeout o estado inesperado al abrir detalle: {outcome}"
        log(f"    -> {last_error}")
        close_popup_with_back(page)
        human_pause(1000, 1800)

    raise RuntimeError(
        f"No fue posible abrir el detalle del radicado {rad}. Último error: {last_error}"
    )


def open_datos_proceso_tab(page):
    # We make sure the 'Datos del Proceso' tab is active before extracting fields.
    candidates = [
        page.get_by_text("DATOS DEL PROCESO", exact=False),
    ]
    click_first_visible(candidates)
    human_pause(500, 1000)
    wait_for_network_idle_soft(page)


def wait_for_detail_core_loaded(page, timeout_ms: int = 15000) -> bool:
    # We wait for the core detail labels before parsing the page text.
    deadline = time.time() + (timeout_ms / 1000)

    labels = [
        page.get_by_text("Fecha de consulta:", exact=False),
        page.get_by_text("Fecha de replicación de datos:", exact=False),
        page.get_by_text("Fecha de Radicación:", exact=False),
        page.get_by_text("Despacho:", exact=False),
        page.get_by_text("Tipo de Proceso:", exact=False),
    ]

    while time.time() < deadline:
        visible_count = 0
        for locator in labels:
            if locator_is_visible(locator):
                visible_count += 1

        if visible_count >= 2:
            return True

        time.sleep(0.25)

    return False


def parse_singlevalue_labeled_text(text: str, field_map: dict, section_headers: set) -> dict:
    # We parse multiline labeled text where each label has a single logical value.
    data = {out_key: "" for out_key in set(field_map.values())}
    current_field = None

    lines = [normalize_spaces(line) for line in safe_str(text).splitlines()]
    lines = [line for line in lines if line]

    for line in lines:
        canon_line = canonical_text(line.rstrip(":"))

        if canon_line in section_headers:
            current_field = None
            continue

        matched_label = False

        if ":" in line:
            left, right = line.split(":", 1)
            canon_label = canonical_text(left)

            if canon_label in field_map:
                current_field = field_map[canon_label]
                data[current_field] = normalize_spaces(right)
                matched_label = True

        if matched_label:
            continue

        if canon_line in field_map:
            current_field = field_map[canon_line]
            continue

        if current_field:
            data[current_field] = normalize_spaces(f"{data[current_field]} {line}")

    return data


def extract_detail_fields_from_current_page(page) -> dict:
    # We extract the detail fields by parsing the visible page text.
    open_datos_proceso_tab(page)
    wait_for_detail_core_loaded(page, timeout_ms=12000)
    human_pause(900, 1600)

    body_text = page.locator("body").inner_text()
    parsed = parse_singlevalue_labeled_text(body_text, DETAIL_FIELD_MAP, DETAIL_SECTION_HEADERS)

    return {
        "fecha_consulta_rama": parsed["fecha_consulta_rama"],
        "fecha_replicacion_datos_rama": parsed["fecha_replicacion_datos_rama"],
        "fecha_radicacion_rama": parsed["fecha_radicacion_rama"],
        "despacho_rama": parsed["despacho_rama"],
        "ponente_rama": parsed["ponente_rama"],
        "tipo_de_proceso_rama": parsed["tipo_de_proceso_rama"],
        "clase_de_proceso_rama": parsed["clase_de_proceso_rama"],
        "subclase_de_proceso_rama": parsed["subclase_de_proceso_rama"],
        "recurso_rama": parsed["recurso_rama"],
        "ubicacion_expediente_rama": parsed["ubicacion_expediente_rama"],
        "contenido_radicacion_rama": parsed["contenido_radicacion_rama"],
    }


def detail_data_score(data: dict) -> int:
    # We score the completeness of the detail extraction.
    important_fields = [
        "fecha_consulta_rama",
        "fecha_replicacion_datos_rama",
        "fecha_radicacion_rama",
        "despacho_rama",
        "tipo_de_proceso_rama",
        "clase_de_proceso_rama",
        "recurso_rama",
        "ubicacion_expediente_rama",
    ]
    return sum(1 for key in important_fields if safe_str(data.get(key, "")))


def reload_detail_page(page):
    # We reload the current detail page to recover from partial frontend loads.
    page.reload(wait_until="domcontentloaded")
    human_pause(1000, 1800)
    wait_for_network_idle_soft(page)
    human_pause(900, 1600)


def extract_detail_fields_with_retries(page, log: LogFn, max_retries: int = 3):
    # We retry the detail extraction until the page looks complete enough.
    last_data = {}

    for attempt in range(1, max_retries + 1):
        log(f"    -> Intento de extraer datos del detalle {attempt}/{max_retries}")

        data = extract_detail_fields_from_current_page(page)
        last_data = data

        score = detail_data_score(data)
        has_core = bool(data["fecha_radicacion_rama"] and data["despacho_rama"])

        if has_core and score >= 5:
            log("    -> Detalle extraído con buena completitud.")
            return data

        log("    -> Detalle incompleto. Se reintentará.")
        if attempt < max_retries:
            reload_detail_page(page)

    return last_data


def wait_for_actuaciones_outcome(page, timeout_ms: int = 22000) -> str:
    # We wait for the actuaciones table, a no-data message, or a transient popup.
    deadline = time.time() + (timeout_ms / 1000)

    while time.time() < deadline:
        popup_kind = detect_popup_kind(page)
        if popup_kind == "network_error":
            return "network_error"

        try:
            table = page.locator("table").filter(has_text="Fecha de Actuación").first
            if table.is_visible():
                return "table"
        except Exception:
            pass

        if detect_no_actuaciones_message(page):
            return "no_data"

        time.sleep(0.25)

    return "timeout"


def extract_first_actuacion_from_table(page):
    # We extract the first row from the actuaciones table and validate it.
    table = page.locator("table").filter(has_text="Fecha de Actuación").first
    table.wait_for(state="visible", timeout=15000)

    first_row = table.locator("tbody tr").first
    first_row.wait_for(state="visible", timeout=15000)

    row_text = safe_str(first_row.inner_text())
    if "no posee actuaciones registradas" in canonical_text(row_text):
        return None, "no_data"

    cells = first_row.locator("td")
    n = cells.count()

    if n < 2:
        return None, "invalid"

    def get_cell(i: int) -> str:
        return safe_str(cells.nth(i).inner_text()) if i < n else ""

    data = {
        "fecha_ultima_actuacion_rama": get_cell(0),
        "ultima_actuacion_rama": get_cell(1),
        "anotacion_rama": get_cell(2),
        "fecha_inicia_termino_rama": get_cell(3),
        "fecha_finaliza_termino_rama": get_cell(4),
        "fecha_registro_actuacion_rama": get_cell(5),
    }

    if "no posee actuaciones registradas" in canonical_text(data["fecha_ultima_actuacion_rama"]):
        return None, "no_data"

    has_date = parse_date_any(data["fecha_ultima_actuacion_rama"]) is not None
    has_action = bool(data["ultima_actuacion_rama"])

    if not has_date and not has_action:
        return None, "invalid"

    return data, "ok"


def extract_actuaciones_with_retries(page, log: LogFn, max_retries: int = 3):
    # We retry the actuaciones extraction several times before concluding anything.
    last_state = "unknown"
    attempts_used = 0

    for attempt in range(1, max_retries + 1):
        attempts_used = attempt
        log(f"    -> Intento de extraer ACTUACIONES {attempt}/{max_retries}")

        candidates = [
            page.get_by_role("tab", name="ACTUACIONES"),
            page.get_by_text("ACTUACIONES", exact=False),
        ]

        if not click_first_visible(candidates):
            raise RuntimeError("No fue posible localizar la pestaña ACTUACIONES.")

        wait_for_network_idle_soft(page)
        human_pause(1500, 2600)

        outcome = wait_for_actuaciones_outcome(page, timeout_ms=22000)
        last_state = outcome

        if outcome == "table":
            data, state = extract_first_actuacion_from_table(page)
            last_state = state

            if state == "ok":
                log("    -> Actuaciones extraídas correctamente.")
                return {
                    "actuaciones_extract_status_rama": "ok",
                    "actuaciones_retries_rama": attempts_used - 1,
                    "data": data,
                }

            log(f"    -> Tabla de actuaciones no válida ({state}). Se reintentará.")
            if attempt < max_retries:
                reload_detail_page(page)
                continue

        if outcome == "network_error":
            log("    -> Network Error al abrir ACTUACIONES. Se reintentará.")
            close_popup_with_back(page)
            if attempt < max_retries:
                human_pause(1200, 2200)
                reload_detail_page(page)
                continue

        if outcome in ("no_data", "timeout"):
            log(f"    -> ACTUACIONES en estado {outcome}. Se reintentará.")
            if attempt < max_retries:
                human_pause(1500, 2600)
                reload_detail_page(page)
                continue

    return {
        "actuaciones_extract_status_rama": last_state,
        "actuaciones_retries_rama": attempts_used - 1,
        "data": {},
    }


def merge_row(base_row: dict, extra_data: dict) -> dict:
    # We preserve original columns and append new ones.
    output = dict(base_row)
    output.update(extra_data)
    return output


def build_base_extra_data(include_compare: bool) -> dict:
    # We create the full Rama payload for each output row.
    data = {
        "popup_multiple_records_rama": False,
        "network_error_retries_rama": 0,
        "selected_result_row_index_rama": "",
        "selected_result_latest_date_rama": "",
        "despacho_departamento_resumen_rama": "",
        "sujetos_procesales_raw_rama": "",
        "demandante_rama": "",
        "demandado_rama": "",
        "ministerio_publico_rama": "",
        "autoridad_conocimiento_rama": "",
        "llamamiento_garantia_rama": "",
        "sin_tipo_sujeto_rama": "",
        "fecha_radicacion_resumen_rama": "",
        "fecha_ultima_actuacion_resumen_rama": "",
        "fecha_consulta_rama": "",
        "fecha_replicacion_datos_rama": "",
        "fecha_radicacion_rama": "",
        "despacho_rama": "",
        "ponente_rama": "",
        "tipo_de_proceso_rama": "",
        "clase_de_proceso_rama": "",
        "subclase_de_proceso_rama": "",
        "recurso_rama": "",
        "ubicacion_expediente_rama": "",
        "contenido_radicacion_rama": "",
        "fecha_ultima_actuacion_rama": "",
        "ultima_actuacion_rama": "",
        "anotacion_rama": "",
        "fecha_inicia_termino_rama": "",
        "fecha_finaliza_termino_rama": "",
        "fecha_registro_actuacion_rama": "",
        "actuaciones_extract_status_rama": "",
        "actuaciones_retries_rama": 0,
        "status_rama": "ok",
        "error_rama": "",
    }

    if include_compare:
        data["fechas_ultima_actuacion_flag"] = False
        data["diferencia_fecha_ultima_actuacion"] = ""

    return data


def normalize_output_dataframe(df: pd.DataFrame, compare_date_col: Optional[str] = None) -> pd.DataFrame:
    # We normalize date-like columns into YYYY-MM-DD when possible.
    date_columns = [
        "selected_result_latest_date_rama",
        "fecha_radicacion_resumen_rama",
        "fecha_ultima_actuacion_resumen_rama",
        "fecha_consulta_rama",
        "fecha_replicacion_datos_rama",
        "fecha_radicacion_rama",
        "fecha_ultima_actuacion_rama",
        "fecha_inicia_termino_rama",
        "fecha_finaliza_termino_rama",
        "fecha_registro_actuacion_rama",
    ]

    if compare_date_col and compare_date_col in df.columns:
        date_columns.append(compare_date_col)

    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(normalize_date_value)

    if compare_date_col and compare_date_col in df.columns and "fecha_ultima_actuacion_rama" in df.columns:
        df["fechas_ultima_actuacion_flag"] = df.apply(
            lambda row: compare_dates_flag(
                row.get(compare_date_col, ""),
                row.get("fecha_ultima_actuacion_rama", ""),
            ),
            axis=1,
        )
        df["diferencia_fecha_ultima_actuacion"] = df.apply(
            lambda row: difference_dates_days(
                row.get(compare_date_col, ""),
                row.get("fecha_ultima_actuacion_rama", ""),
            ),
            axis=1,
        )

    return df


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    # We build the downloadable Excel file entirely in memory.
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="resultado")
    buffer.seek(0)
    return buffer.getvalue()


def read_uploaded_dataframe(file_bytes: bytes, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    # We read CSV and Excel uploads into a string-typed dataframe.
    lower_name = filename.lower()
    buffer = io.BytesIO(file_bytes)

    if lower_name.endswith(".csv"):
        encodings = ["utf-8-sig", "utf-8", "latin-1"]
        last_error = None
        for encoding in encodings:
            try:
                buffer.seek(0)
                return pd.read_csv(buffer, dtype=str, keep_default_na=False, encoding=encoding)
            except Exception as exc:
                last_error = exc
        raise ValueError(f"No se pudo leer el CSV. Último error: {last_error}")

    if lower_name.endswith(".xlsx") or lower_name.endswith(".xls"):
        buffer.seek(0)
        return pd.read_excel(buffer, dtype=str, keep_default_na=False, sheet_name=sheet_name or 0)

    raise ValueError("Formato no soportado. Usa CSV, XLSX o XLS.")


def list_excel_sheets(file_bytes: bytes, filename: str) -> list[str]:
    # We return the available sheet names when the upload is an Excel workbook.
    lower_name = filename.lower()
    if not (lower_name.endswith(".xlsx") or lower_name.endswith(".xls")):
        return []

    workbook = pd.ExcelFile(io.BytesIO(file_bytes))
    return list(workbook.sheet_names)


def process_dataframe(
    df: pd.DataFrame,
    radicado_col: str,
    compare_date_col: Optional[str] = None,
    log: Optional[LogFn] = None,
    progress: Optional[ProgressFn] = None,
    headless: bool = True,
) -> pd.DataFrame:
    # We run the complete Rama extraction pipeline over the uploaded dataframe.
    log = log or (lambda _: None)
    progress = progress or (lambda current, total, rad: None)

    if radicado_col not in df.columns:
        raise ValueError(f"La columna de radicado '{radicado_col}' no existe en el archivo.")

    if compare_date_col and compare_date_col not in df.columns:
        raise ValueError(f"La columna de fecha '{compare_date_col}' no existe en el archivo.")

    resultados = []
    total = len(df)
    include_compare = bool(compare_date_col)
    consecutive_search_network_failures = 0

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/pw-browsers")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        page = browser.new_page()
        page.set_viewport_size({"width": 1400, "height": 900})

        reset_to_search(page)

        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            base_row = {col: safe_str(row[col]) for col in df.columns}
            rad = safe_str(base_row.get(radicado_col, ""))
            progress(idx, total, rad)
            log(f"[{idx}/{total}] Radicación: {rad}")

            extra_data = build_base_extra_data(include_compare=include_compare)

            if not is_valid_radicado(rad):
                extra_data["status_rama"] = "radicado_invalido"
                extra_data["error_rama"] = "El radicado no tiene exactamente 23 dígitos numéricos."
                resultados.append(merge_row(base_row, extra_data))
                log("    -> Radicado inválido. Se omite.")
                continue


            try:
                search_info = search_with_retries(page, rad, log=log, max_retries=MAX_SEARCH_RETRIES)
                extra_data.update(search_info)
                consecutive_search_network_failures = 0

                detail_open_info = open_detail_with_retries(
                    page,
                    rad,
                    log=log,
                    max_retries=MAX_DETAIL_OPEN_RETRIES,
                )
                extra_data.update(detail_open_info)

                detail_fields = extract_detail_fields_with_retries(
                    page,
                    log=log,
                    max_retries=MAX_DETAIL_DATA_RETRIES,
                )
                extra_data.update(detail_fields)

                act_result = extract_actuaciones_with_retries(
                    page,
                    log=log,
                    max_retries=MAX_ACTUACIONES_RETRIES,
                )
                extra_data["actuaciones_extract_status_rama"] = act_result[
                    "actuaciones_extract_status_rama"
                ]
                extra_data["actuaciones_retries_rama"] = act_result["actuaciones_retries_rama"]

                if act_result["data"]:
                    extra_data.update(act_result["data"])
                else:
                    if extra_data["fecha_ultima_actuacion_resumen_rama"]:
                        extra_data["fecha_ultima_actuacion_rama"] = extra_data[
                            "fecha_ultima_actuacion_resumen_rama"
                        ]
                        extra_data["status_rama"] = "actuaciones_no_confirmadas"
                        extra_data["error_rama"] = (
                            "No se logró extraer la tabla de actuaciones tras reintentos, "
                            "aunque el resumen sí reportó última actuación."
                        )
                    else:
                        extra_data["status_rama"] = "sin_actuaciones"
                        extra_data["error_rama"] = (
                            "Luego de reintentos, el registro no mostró actuaciones extraíbles."
                        )

            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"

                if "NETWORK_ERROR_PERSISTENT" in str(exc):
                    consecutive_search_network_failures += 1
                    extra_data["status_rama"] = "network_error"
                    extra_data["error_rama"] = (
                        "La consulta devolvió 'Network Error' en todos los reintentos. "
                        "Posible bloqueo o indisponibilidad del portal desde el entorno de despliegue."
                    )
                    log(
                        "    -> Falla persistente de red al consultar Rama. "
                        f"Consecutivas: {consecutive_search_network_failures}/"
                        f"{MAX_CONSECUTIVE_SEARCH_NETWORK_FAILURES}."
                    )
                else:
                    consecutive_search_network_failures = 0
                    extra_data["status_rama"] = "error"
                    extra_data["error_rama"] = message

            resultados.append(merge_row(base_row, extra_data))

            if consecutive_search_network_failures >= MAX_CONSECUTIVE_SEARCH_NETWORK_FAILURES:
                raise RuntimeError(
                    "Se detectaron fallas de red persistentes en consultas consecutivas a Rama Judicial. "
                    "La ejecución se detiene para evitar reprocesar todo el archivo sin resultados. "
                    "Reintenta más tarde o despliega en otro proveedor/región."
                )
            human_pause(1200, 2800)

        browser.close()

    out_df = pd.DataFrame(resultados)
    out_df = normalize_output_dataframe(out_df, compare_date_col=compare_date_col)
    return out_df
