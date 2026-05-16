# Proyecto Final SBD

Proyecto de Ingenieria de Datos y Ciencia de Datos para el procesamiento, analisis y visualizacion de datasets medicos multi-modales (clinicos, genomicos y de imagenes de ultrasonido) enfocados en el cancer de mama en Africa.

## Descripcion General

El proyecto implementa un pipeline de datos completo, siguiendo la metodologia CRISP-DM y una arquitectura de Data Lake por capas. El objetivo es ingerir datos de diversas fuentes, limpiarlos, prepararlos para modelos de Machine Learning, extraer metricas relevantes y proporcionar herramientas de visualizacion, tanto estaticas como interactivas.

## Arquitectura del Data Lake

Los datos fluyen a traves de tres capas principales ubicadas en la carpeta `datalake/`:

1. **Raw Layer**: Almacenamiento inicial de los datos tal como provienen de la fuente (CSV, Parquet, metadatos extraidos de imagenes crudas).
2. **Cleanse Layer**: Datos limpios y estandarizados. Se aplican transformaciones como el tratamiento de nulos, clampeo de outliers y particionado logico.
3. **Curated Layer**: Datos enriquecidos y estructurados para Machine Learning. Incluye reduccion de dimensionalidad (PCA), segmentacion (K-Means), deteccion de anomalias (Isolation Forest) y seleccion de variables importantes.

## Estructura del Proyecto

```text
Proyecto_Final/
├── datalake/                # Data Lake local organizado por capas
│   ├── raw/                 # Datos originales (csv/parquet/imágenes)
│   ├── cleanse/             # Datos limpios por dominio (clinical/, genomics/, images/)
│   │   ├── clinical/
│   │   ├── genomics/
│   │   └── images/
│   ├── curated/             # Datos preparados para ML y visualización
│   │   ├── clinical/
│   │   ├── clinical_classification/
│   │   ├── genomics/
│   │   ├── genomics_anomalies/
│   │   ├── genomics_clustering/
│   │   └── images/
│   └── powerbi/             # Artefactos para Power BI (ej. clinical_data_full.csv)
├── jobs/                    # Scripts del pipeline (números indican el orden recomendado)
├── reports/                 # Salidas analíticas y visuales
│   ├── metrics/             # Métricas en formato JSON
│   └── figures/             # Visualizaciones estáticas generadas
├── logs/                    # Resúmenes y logs de ejecución (json)
├── docs/                    # Documentación y metodología
├── tests/                   # Tests unitarios (p.ej. test_datalake.py)
├── memoria.md               # Memoria técnica detallada paso a paso
├── docker-compose.yml       # Configuración de la infraestructura Docker
└── requirements.txt         # Dependencias de Python
```

## Ejecución del Pipeline

Los scripts en `jobs/` están numerados para indicar el orden recomendado. Se puede ejecutar cada paso individualmente o dentro del contenedor Docker (recomendado para reproducibilidad).

1) Levantar entorno Docker (opcional, recomendable):

```bash
docker-compose up -d
```

2) Ejecutar los pasos en orden (ejemplo mínimo, desde la raíz del proyecto):

```bash
python jobs/01_ingesta_clinical.py
python jobs/02_ingesta_genomics.py
python jobs/03_ingesta_images.py

python jobs/04_limpieza_clinical.py
python jobs/05_limpieza_genomics.py
python jobs/06_limpieza_images.py

python jobs/07_curated_clasificacion.py
python jobs/08_curated_clustering.py
python jobs/09_curated_anomalias.py

python jobs/10_metrics_clinical.py
python jobs/11_metrics_genomics.py
python jobs/12_metrics_images.py

python jobs/13_viz_clinical.py
python jobs/14_viz_genomics.py
python jobs/15_viz_images.py

# Dashboard / export a Power BI
python jobs/16_dashboard.py
python jobs/17_export_powerbi.py
```

Notas importantes:
- Los scripts escriben salidas en `datalake/curated/`, `reports/metrics/` y `reports/figures/`.
- Si prefieres ejecutar local sin Docker, instala dependencias con:

```bash
pip install -r requirements.txt
```

- Revisa `logs/` para los resúmenes de ejecución (archivos json con timestamps).
- Hay tests básicos en `tests/test_datalake.py`; ejecútalos con `pytest tests/`.

## Dashboard Interactivo y Export a Power BI (Fase 6)

El proyecto incluye un dashboard interactivo (Plotly Dash) y una tarea para exportar artefactos compatibles con Power BI.

Iniciar dashboard:
```bash
python jobs/16_dashboard.py
```
El servidor estará disponible en `http://localhost:8050/`.

Exportar datos para Power BI (genera `datalake/powerbi/` con archivos CSV usados por los informes):
```bash
python jobs/17_export_powerbi.py
```

## Requisitos y Configuracion

### Entorno Docker
Para levantar el entorno principal con Docker:
```bash
docker-compose up -d
```
El entorno de trabajo se montara automaticamente en el directorio `/work` y el Data Lake en `/datalake`.

### Entorno Local
Para ejecutar los scripts fuera de Docker, es necesario instalar las dependencias:
```bash
pip install -r requirements.txt
```
Todos los scripts estan preparados para detectar dinamicamente la ruta del datalake, funcionando sin problemas independientemente de si se ejecutan en local o en el contenedor.
