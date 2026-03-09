# Arquitectura del flujo de procesamiento

Este diagrama describe el flujo de procesamiento de archivos y consulta en la Rama Judicial.

```mermaid
flowchart TD
    A[Usuario sube archivo CSV/XLSX/XLS] --> B[App lee archivo y detecta hojas si es Excel]
    B --> C[Usuario elige columna de radicado]
    C --> D{¿Comparar fecha última actuación?}
    D -->|Sí| E[Usuario elige columna de fecha base]
    D -->|No| F[Continuar sin comparación]
    E --> G[Iniciar ejecución]
    F --> G[Iniciar ejecución]

    G --> H[Validar/instalar Chromium para Playwright]
    H --> I[Recorrer filas del DataFrame]
    I --> J{¿Radicado válido? 23 dígitos}
    J -->|No| K[Marcar status_rama = radicado_invalido]
    J -->|Sí| L[Consultar Rama Judicial con reintentos]

    L --> M[Detectar resultados/popup/error de red]
    M --> N[Elegir mejor fila por fecha más reciente]
    N --> O[Abrir detalle del proceso]
    O --> P[Extraer Datos del Proceso]
    P --> Q[Ir a pestaña ACTUACIONES]
    Q --> R{¿Se pudo extraer primera actuación?}
    R -->|Sí| S[Guardar fecha y detalle de actuación]
    R -->|No| T[Usar resumen o marcar sin_actuaciones/actuaciones_no_confirmadas]

    K --> U[Unir datos originales + columnas _rama]
    S --> U
    T --> U

    U --> V[Normalizar fechas a YYYY-MM-DD]
    V --> W{¿Comparación de fechas activa?}
    W -->|Sí| X[Calcular flag e diferencia de días]
    W -->|No| Y[Omitir comparación]
    X --> Z[Generar Excel resultado en memoria]
    Y --> Z[Generar Excel resultado en memoria]
    Z --> AA[Mostrar preview + resumen status_rama]
    AA --> AB[Habilitar descarga del Excel]