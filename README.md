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
├── datalake/           # Almacenamiento local organizado por capas
│   ├── raw/
│   ├── cleanse/
│   └── curated/
├── jobs/               # Scripts secuenciales del pipeline de datos
├── reports/            # Salidas analiticas
│   ├── metrics/        # Metricas en formato JSON
│   └── figures/        # Visualizaciones estaticas generadas
├── memoria.md          # Memoria tecnica detallada paso a paso
├── docker-compose.yml  # Configuracion de la infraestructura Docker
└── requirements.txt    # Dependencias de Python
```

## Ejecucion del Pipeline

El proyecto esta disenado para ejecutarse de forma secuencial a traves de los scripts ubicados en la carpeta `jobs/`. Se recomienda ejecutar el proyecto dentro de su contenedor Docker para garantizar la coherencia del entorno.

### 1. Ingesta (Fase 2)
* `01_ingesta_clinical.py`
* `02_ingesta_genomics.py`
* `03_ingesta_images.py`

### 2. Procesamiento y Limpieza (Fase 3)
* `04_limpieza_clinical.py`
* `05_limpieza_genomics.py`
* `06_limpieza_images.py`

### 3. Capa Curated y Machine Learning (Fase 3)
* `07_curated_clasificacion.py`
* `08_curated_clustering.py`
* `09_curated_anomalias.py`

### 4. Metricas Analiticas (Fase 4)
* `10_metrics_clinical.py`
* `11_metrics_genomics.py`
* `12_metrics_images.py`

### 5. Reportes Graficos (Fase 5)
* `13_viz_clinical.py`
* `14_viz_genomics.py`
* `15_viz_images.py`

## Dashboard Interactivo (Fase 6)

El proyecto incluye un cuadro de mando interactivo construido con Plotly Dash que consolida los hallazgos visuales.

Para iniciarlo:
```bash
python jobs/16_dashboard.py
```
El servidor estara disponible en la direccion `http://localhost:8050/`.

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
