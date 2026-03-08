# Consulta Rama Judicial - Streamlit

Esta app permite:

- subir un archivo CSV, XLSX o XLS
- elegir la columna del radicado
- opcionalmente elegir una columna de fecha de última actuación para comparar
- consultar Rama Judicial en modo headless con Playwright
- descargar un Excel con las columnas originales más las columnas `_rama`

## Estructura

- `app.py`: interfaz Streamlit
- `rama_scraper.py`: lógica de lectura, scraping y generación del Excel
- `requirements.txt`: dependencias Python
- `packages.txt`: dependencias del sistema para Chromium

## Local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

## Deploy en Streamlit Community Cloud

1. Subir esta carpeta a un repo de GitHub.
2. Crear la app en Streamlit Community Cloud apuntando a `app.py`.
3. Si la instalación de Chromium falla en Streamlit Cloud, el siguiente paso recomendado es mover el deploy a Render con Docker.

## Notas

- La app intenta instalar Chromium automáticamente en runtime si hace falta.
- Los logs viven solo durante la sesión activa del usuario.
- Para Excel con varias hojas, la app deja seleccionar la hoja.
