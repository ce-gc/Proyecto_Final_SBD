# Memoria Técnica del Proyecto

## Registro de Actividades

### Fase 1 — Planificación y metodología
Se ha procedido con la implementación de la infraestructura básica del proyecto, la selección de la metodología de trabajo y la definición del almacenamiento persistente.

**Hitos logrados:**
- **Selección de metodología**: Adopción de CRISP-DM para guiar el ciclo de vida del proyecto.
- **Diseño de arquitectura por capas**: Implementación de un Data Lake con capas Raw, Cleanse y Curated.
- **Definición del almacenamiento**: Configuración de volúmenes persistentes y estructura de carpetas estandarizada.
- **Orquestación con Docker Compose**: Creación del entorno de contenedores para garantizar la reproducibilidad y facilitar el despliegue.

**Estructura principal de carpetas:**
```text
Proyecto_Final/
├── .devcontainer/
├── datalake/
│   ├── raw/
│   ├── cleanse/
│   └── curated/
├── docs/
├── jobs/
├── reports/
│   ├── metrics/
│   └── figures/
├── tests/
```

### Fase 2 — Ingesta de datos (Raw Layer)
Se ha iniciado la fase de ingesta para mover los datos desde las fuentes originales hacia la capa Raw, asegurando la consistencia y validando los esquemas iniciales.

**Hitos logrados:**
- **Lectura del CSV y Parquet con PySpark**: Implementación de lógica de validación de esquemas y detección de variaciones en los datos clínicos y genómicos.
- **Procesado de imágenes**: Desarrollo de la lógica para recorrer directorios y extraer metadatos de imágenes de ultrasonido.
- **Creación de jobs de ingesta**:
    - `01_ingesta_clinical.py`: Ingesta del dataset clínico (CSV).
    - `02_ingesta_genomics.py`: Ingesta del dataset genómico (Parquet).
    - `03_ingesta_images.py`: Ingesta y extracción de metadatos de imágenes.

**Correcciones aplicadas durante la validación:**
- **Rutas duales Docker/local**: Los jobs resuelven automáticamente si se ejecutan dentro del contenedor (`/datalake`) o en local (ruta relativa al proyecto). La versión inicial tenía rutas que solo funcionaban en un contexto.
- **Consistencia en `docker-compose.yml`**: El volumen de trabajo se montaba en `/app` pero el Dockerfile definía `WORKDIR /work`. Se unificó a `/work` y se añadieron `stdin_open` y `tty` para mantener el contenedor activo.
- **Dependencia Pillow**: Se añadió `Pillow` al Dockerfile y al `requirements.txt` para poder extraer metadatos de las imágenes (dimensiones, modo de color).
- **Idempotencia**: Los tres jobs verifican la existencia de la salida antes de escribir, evitando duplicados en ejecuciones repetidas.
- **Protección contra handlers duplicados**: Se evita la acumulación de handlers de logging si el job se ejecuta varias veces en la misma sesión de Python.
- **Detección de imágenes corruptas**: El job de imágenes usa `img.verify()` para detectar archivos dañados antes de extraer metadatos.

**Mejoras de robustez (schema evolution y variaciones de datos):**

Los tres jobs fueron refactorizados para soportar cualquier variación en los datos de entrada sin abortar la ingesta:

- **CSV clínico (`01_ingesta_clinical.py`)**:
  - Lectura en modo `PERMISSIVE` con captura de filas corruptas en `_corrupt_record`.
  - Columnas faltantes se rellenan con `null` (no se aborta la ingesta).
  - Columnas extra se conservan (schema evolution).
  - Cast automático de tipos string→DoubleType en columnas numéricas.
  - Deduplicación por columna `id`.
  - Detección de ficheros vacíos (0 bytes) antes de la lectura.
  - Soporte para CSV multiline, encoding UTF-8 y escape de comillas.
- **Parquet genómico (`02_ingesta_genomics.py`)**:
  - Lectura segura con fallback: si el Parquet principal falla, intenta lectura de fragmentos válidos con `unionByName(allowMissingColumns=True)`.
  - Configuración `ignoreCorruptFiles=true` y `mergeSchema=true`.
  - Columnas críticas ausentes se rellenan con `null` (no se aborta).
  - Validación de rangos numéricos (`age_at_diagnosis`, `survival_months`, `recurrence_free_months`).
  - Deduplicación por todas las columnas.
  - Schema snapshot guardado como JSON para trazabilidad de evolución entre ejecuciones.
- **Imágenes de ecografía (`03_ingesta_images.py`)**:
  - Categorías dinámicas: cualquier subdirectorio se acepta como categoría válida (no se restringen a un set fijo).
  - Formatos ampliados: PNG, JPG, JPEG, BMP, TIFF, WEBP, GIF.
  - `Image.MAX_IMAGE_PIXELS = None` para soportar imágenes médicas de alta resolución.
  - Verificación en dos fases: `img.verify()` + fallback a lectura directa (WEBP/TIFF no soportan verify).
  - Detección de ficheros vacíos (0 bytes).
  - Estadísticas de dimensiones (min/max/media por width y height, tamaños únicos).
  - Detección de duplicados por hash MD5 del contenido.
  - 10 campos de metadatos (vs. 7 anteriores): + `channels`, `format`, `file_hash`.

**Resultados de la ejecución (validados en contenedor Docker):**

| Job | Registros | Columnas | Errores | Resultado |
|-----|-----------|----------|---------|-----------|
| `01_ingesta_clinical.py` | 569 filas | 32 (id + diagnosis + 30 numéricas) | 0 corruptos, 0 duplicados | Parquet generado en `raw/clinical/` |
| `02_ingesta_genomics.py` | 100,000 filas | 130 (10 críticas + 120 extra) | 0 duplicados, 7 columnas con nulls | Fichero original preservado + schema snapshot |
| `03_ingesta_images.py` | 1,578 imágenes | 10 metadatos por imagen | 0 corruptas, 3 duplicados (hash) | Parquet generado en `raw/images_meta/` |

**Detalle de nulls detectados en dataset genómico (informativo, no bloqueante):**
- `age_at_diagnosis`: 497 nulls (0.5%)
- `molecular_subtype`: 974 nulls (1.0%)
- `survival_months`: 10,252 nulls (10.3%)
- `vital_status`: 4,901 nulls (4.9%)
- `recurrence_free_months`: 12,159 nulls (12.2%)
- `recurrence_event`: 7,939 nulls (7.9%)
- `distant_metastasis`: 11,770 nulls (11.8%)

**Validación de rangos en dataset genómico:**
- `age_at_diagnosis`: todos los valores en rango [0, 120] ✓
- `survival_months`: 44,471 valores fuera de rango [0, 600] (datos sintéticos)
- `recurrence_free_months`: 43,570 valores fuera de rango [0, 600] (datos sintéticos)

**Distribución de imágenes por categoría:**
- Benign: 891 imágenes
- Malignant: 421 imágenes
- Normal: 266 imágenes

**Estadísticas de dimensiones de imágenes:**
- Width: min=190, max=1048, media=616.1
- Height: min=310, max=719, media=501.6
- Tamaños únicos: 639
- Modos de color: `1` (binario), `RGB`, `RGBA`
- Canales: 1, 3, 4
- Duplicados detectados por hash MD5: 3

### Fase 3 — Procesado y arquitectura por capas (Cleanse Layer)
Se ha implementado la capa de limpieza para transformar los datos brutos de la capa Raw en un formato estandarizado, limpio y listo para el análisis, siguiendo la arquitectura de capas del Data Lake.

**Hitos logrados:**
- **Creación de la capa Cleanse**: Implementación de jobs para el tratamiento de nulos, detección de outliers y normalización de variables.
- **Particionamiento de datos**: Uso de `partitionBy` para organizar los datos limpios físicamente en el disco según categorías clave, optimizando el acceso a los datos.
- **Trazabilidad y auditoría**: Inclusión de columnas técnicas (`_clean_ts`, `_clean_job`) en todos los datasets procesados para asegurar el linaje de los datos.

**Detalle de los jobs de limpieza:**

- **Limpieza clínica (`04_limpieza_clinical.py`)**:
    - **Acciones**: Normalización de la columna `diagnosis` (M/B), cast de 30 columnas numéricas a `DoubleType` e imputación de nulos con la mediana.
    - **Tratamiento de outliers**: Aplicación de la técnica IQR (rango intercuartílico) con factor 1.5 para clampear valores extremos, asegurando que los modelos no se vean sesgados por valores atípicos.
    - **Estructura**: Particionado físico por `diagnosis` (M/B).
    - **Resultados**: 688 valores clampeados en 29 columnas.
- **Limpieza genómica (`05_limpieza_genomics.py`)**:
    - **Acciones**: Limpieza de strings (trim/upper) en variables categóricas, imputación de nulos numéricos (edad, supervivencia) con la mediana y relleno de nulos categóricos con `"UNKNOWN"`.
    - **Estructura**: Particionado físico por `molecular_subtype`.
    - **Resultados**: Imputación de más de 20,000 nulos en métricas de supervivencia y recurrencia.
- **Limpieza de metadatos de imágenes (`06_limpieza_images.py`)**:
    - **Acciones**: Eliminación de duplicados por hash MD5 (3 detectados), filtrado de dimensiones inválidas y validación de categorías.
    - **Estructura**: Particionado físico por `category` (`normal`, `benign`, `malignant`).

**Problemas encontrados y soluciones:**
- **Heterogeneidad en tipos de datos**: Se detectó que algunas columnas numéricas venían como string en la capa raw; se implementó un cast preventivo en los jobs de limpieza.
- **Valores extremos en datos sintéticos**: El dataset genómico presentaba outliers significativos. En lugar de eliminar filas, se optó por un clampeo estadístico para no perder volumen de datos valioso.
- **Estructura de carpetas física**: Se configuró Spark para que la estructura en `cleanse/` refleje las particiones lógicas (`category=normal`, etc.), lo que permite a los data scientists cargar solo las categorías de interés.

**Resultados de la ejecución de limpieza:**

| Job | Filas In | Filas Out | Estado |
|-----|----------|-----------|--------|
| `04_limpieza_clinical.py` | 569 | 569 | Finalizado con éxito ✓ |
| `05_limpieza_genomics.py` | 100,000 | 100,000 | Finalizado con éxito ✓ |
| `06_limpieza_images.py` | 1,578 | 1,575 | Finalizado con éxito ✓ |

**Preparación para análisis avanzado (Curated Layer)**
Se ha procedido a la creación de la capa Curated, donde los datos se transforman y estructuran específicamente para alimentar modelos de Machine Learning y análisis estadísticos avanzados.

**Hitos logrados:**
- **Preparación para Clasificación**: Creación de un dataset clínico escalado y extracción de la importancia de las variables para el diagnóstico.
- **Preparación para Clustering**: Reducción de dimensionalidad mediante PCA y segmentación de pacientes mediante K-Means en el dataset genómico.
- **Detección de Anomalías**: Identificación de registros atípicos (top 1%) en los datos genómicos para asegurar la calidad del análisis posterior.

**Detalle de los jobs de la capa Curated:**

- **Clasificación Clínica (`07_curated_clasificacion.py`)**:
    - **Acciones**: Escalado de características con `StandardScaler` y entrenamiento de un Random Forest ligero para identificar las variables más influyentes.
    - **Resultados**: Se generó un archivo de metadatos con el Top 5 de variables (ej. `radius_worst`, `perimeter_worst`).
- **Clustering Genómico (`08_curated_clustering.py`)**:
    - **Acciones**: Aplicación de PCA (k=2) sobre las 130 columnas de expresión y ejecución de K-Means (k=3) para asignar un `cluster_id` a cada paciente.
    - **Resultados**: Dataset listo para visualización tipo Scatter Plot.
- **Detección de Anomalías Genómicas (`09_curated_anomalias.py`)**:
    - **Acciones**: Cálculo de un score de anomalía basado en la distancia en el espacio PCA y filtrado del percentil 99.
    - **Resultados**: Identificación de los casos más inusuales del dataset genómico.

**Correcciones aplicadas:**
- **Prevención de Data Leakage**: En el Job 07 de clasificación, se introdujo una división de datos (Train/Test) *antes* de aplicar el `StandardScaler`. El escalador y el modelo de Random Forest se ajustan (`fit`) exclusivamente sobre el set de entrenamiento, evitando que el modelo conozca la distribución de los datos de prueba. Ambos sets se unen para guardarse particionados por `dataset_split`.
- **Filtrado dinámico de tipos**: Se mejoraron los jobs para detectar automáticamente solo las columnas numéricas (`double`, `float`, `int`) para los ensambladores de vectores, evitando fallos con columnas categóricas de metadatos.
- **Manejo de nulos en vectores**: Se configuró `handleInvalid="skip"` en los procesos de ML para garantizar que los modelos solo se entrenen con registros completos y válidos.

**Resultados de la capa Curated:**

| Job | Entrada (Cleanse) | Salida (Curated) | Estado |
|-----|-------------------|------------------|--------|
| `07_curated_clasificacion.py` | `clinical` | `clinical_classification` | Finalizado ✓ |
| `08_curated_clustering.py` | `genomics` | `genomics_clustering` | Finalizado ✓ |
| `09_curated_anomalias.py` | `genomics` | `genomics_anomalies` | Finalizado ✓ |

### Fase 4 — Análisis avanzado
En esta fase se han implementado scripts de generación de métricas detalladas para cada dataset, consolidando los resultados en la capa de salida `Results` para su posterior visualización.

**Métricas utilizadas y justificación:**

- **Dataset Clínico (`10_metrics_clinical.py`)**:
    - **Métricas**: Estadísticas descriptivas por clase, Ratio de Fisher, Correlación de Pearson y evaluación de Random Forest (Accuracy, F1, AUC-ROC, Matriz de Confusión).
    - **Justificación**: El **Ratio de Fisher** permite cuantificar la capacidad de separación de cada variable antes del modelo. El uso de **RF con split 80/20** y **escalado** asegura que las métricas de rendimiento sean realistas y comparables con el job de la capa Curated.
- **Dataset Genómico (`11_metrics_genomics.py`)**:
    - **Métricas**: Distribución geográfica, Inercia (Codo), Silhouette Score y Anomaly Score (Isolation Forest).
    - **Justificación**: Se aplicó **PCA (k=2)** para el clustering para reducir el ruido de las 130 variables de expresión, permitiendo identificar patrones biológicos claros. Para la detección de anomalías, se utilizó **PCA (k=5)** e **Isolation Forest** con una contaminación del 1% para aislar los perfiles más atípicos.
- **Dataset de Imágenes (`12_metrics_images.py`)**:
    - **Métricas**: Balance de clases, estadísticas de dimensiones (width/height), ratio de aspecto y estadísticas de brillo.
    - **Justificación**: El **balance de clases** es crítico para evitar sesgos en modelos de visión. El **ratio de aspecto** detecta deformaciones en las capturas que podrían afectar al entrenamiento de redes neuronales.

**Análisis de consistencia y errores corregidos:**

Durante el desarrollo de los scripts de métricas, se detectaron y resolvieron los siguientes problemas técnicos:

- **Inconsistencia Algorítmica**: Se detectó que los jobs de la capa `curated` aplicaban transformaciones (como PCA y escalado) que no estaban inicialmente en los scripts de métricas. Se corrigió añadiendo `StandardScaler` en el script clínico y `PCA` (k=2 y k=5) en el genómico para garantizar que las métricas evaluadas correspondan exactamente al procesamiento de la capa Curated.
- **Optimización de Rendimiento**: Al trabajar con 100,000 registros genómicos, el cálculo del **Silhouette Score** (complejidad $O(N^2)$) bloqueaba el proceso. Se solucionó implementando un **muestreo aleatorio (sampling)** de 1,000 registros para el cálculo de siluetas, manteniendo la representatividad estadística con un tiempo de ejecución eficiente.
- **Dependencias y Entorno**: Se identificó la falta de `scikit-learn` en el entorno local y en `requirements.txt`. Se actualizaron las dependencias y se adaptaron los scripts para leer Parquet mediante `pandas` y `pyarrow`, evitando dependencias de Spark en el nivel de reporte final.

**Problemas detectados al usar datos de `/curated`:**

- **Reducción excesiva de datos**: Los archivos en `/curated` (especialmente clasificación y clustering) solo guardaban los vectores de características y los IDs. Esto impedía generar métricas descriptivas legibles (medias de variables originales) o matrices de correlación. 
- **Solución**: Los scripts de métricas cargan los datos desde `/cleanse` (que conserva todas las columnas originales tras la limpieza) y replican las transformaciones de `/curated` (escalado/PCA) en memoria para realizar el análisis avanzado.

**Breve análisis de resultados (JSON):**

- **Clínico**: Variables de tamaño (`radius_mean`, `area_mean`) muestran una separación clara, con valores significativamente más altos en casos malignos. El modelo alcanza un **AUC-ROC superior a 0.98**, demostrando la alta calidad de las variables seleccionadas.
- **Genómico**: La distribución geográfica muestra una muestra diversa y equilibrada. Los clusters presentan perfiles de expresión diferenciados, validando la segmentación de subtipos moleculares.
- **Imágenes**: Existe un ligero desbalance (56% benigno vs 26% maligno), lo que sugiere que el futuro modelo de IA deberá compensar este peso mediante *class weights* o *data augmentation*. La variabilidad en dimensiones confirma la necesidad de un paso de *resizing* estándar.

### Fase 5 — Representación gráfica
Se ha desarrollado un conjunto de scripts en Python (`13_viz_clinical.py`, `14_viz_genomics.py` y `15_viz_images.py`) dedicados a la generación de visualizaciones a partir de los datos procesados. El objetivo es ofrecer una interpretación gráfica clara, estética y fundamentada clínicamente para cada uno de los tres datasets.

**Visualizaciones generadas por dataset:**

- **Clínico (`13_viz_clinical.py`)**:
    - **Matriz de Correlación (Heatmap)**: Identifica variables altamente correlacionadas que pueden ser descartadas para evitar multicolinealidad.
    - **Boxplots por Diagnóstico**: Visualiza la separabilidad de clases en las 6 variables con mayor ratio de Fisher, validando su poder predictivo.
    - **Feature Importance (Barplot)**: Muestra el top 15 de variables morfológicas más importantes según un modelo Random Forest.
    - **Curva ROC**: Evalúa el rendimiento del clasificador clínico graficando la tasa de verdaderos positivos frente a falsos positivos (anotando el AUC).
    - **Matriz de Confusión**: Expone los verdaderos y falsos positivos/negativos tanto en valores absolutos como porcentuales, resaltando la tasa de falsos negativos.

- **Genómico (`14_viz_genomics.py`)**:
    - **Elbow Plot (Codo)**: Justifica la elección de K=3 clusters visualizando la inercia del modelo K-Means frente a distintos valores de K.
    - **Scatter Plot PCA 2D**: Proyecta la segmentación de pacientes (clusters) en las dos componentes principales, mostrando la varianza explicada.
    - **Composición de Clusters por País (Barplot apilado)**: Evalúa el impacto o la distribución geográfica en los perfiles genómicos descubiertos.
    - **Distribución de Anomaly Scores (Histograma)**: Muestra las anomalías detectadas por Isolation Forest, destacando con una línea el umbral y en rojo el percentil inusual.

- **Imágenes (`15_viz_images.py`)**:
    - **Balance de Clases (Pie chart)**: Muestra porcentualmente las categorías (Normal, Benign, Malignant) de cara a justificar técnicas de balanceo futuro.
    - **Scatter de Dimensiones**: Representa el ancho frente al alto de cada imagen, útil para detectar anomalías de resolución sistemáticas y outliers de tamaño.

**Almacenamiento e Interpretación:**
Todas las figuras generadas se han exportado en formato PNG de alta resolución (150 dpi) y se han almacenado en el directorio estructurado `reports/figures/`. Además, para facilitar la redacción del informe final, cada imagen está acompañada de un archivo de texto (`.txt`) homónimo que contiene la justificación y la interpretación clínica/técnica de esa visualización concreta.

**Dificultades encontradas y soluciones técnicas:**

- **Dependencias de visualización ausentes**: Al intentar ejecutar los scripts de visualización, se detectó la ausencia de las librerías `matplotlib` y `seaborn` en el entorno. Se procedió a su instalación mediante el gestor de paquetes. Además, se implementó una paleta de colores global y consistente para todas las visualizaciones a nivel de código para mantener la estética cruzada entre scripts.
- **Advertencias (Warnings) de versiones recientes de Seaborn**: Durante la generación de los boxplots en los scripts clínico y de imágenes, la librería arrojaba advertencias de futura deprecación por asignar el parámetro `palette` sin `hue`. Se arregló modificando el código para auto-asignar `hue` a la misma variable del eje X y configurando `legend=False`, logrando la misma estética y evitando la ruptura del código en versiones venideras.
- **Gráfico condicional de intensidad de píxeles**: El requerimiento contemplaba un boxplot con el brillo medio siempre y cuando hubiera sido calculado en capas previas. Dado que la capa Cleanse se focalizó en los metadatos y resoluciones espaciales sin llegar a procesar intensidades a nivel de píxel, se codificó una comprobación condicional dinámica en el script. Si la métrica no existe, se omite de forma limpia el gráfico; si en un futuro la métrica es añadida al pipeline, se generará sin necesidad de alterar este script.

---
*(Este archivo se continuará actualizando con las siguientes fases del proyecto.)*
