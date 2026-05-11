# Descripción de los Datasets

El proyecto integra tres fuentes de datos heterogéneas relacionadas con el cáncer de mama. Esta integración permite realizar análisis cruzados entre datos clínicos, genómicos e imagenológicos, proporcionando una visión holística de la patología.

---

## 2.1 Dataset 1 — Datos clínicos (CSV)
**Fuente:** *Breast Cancer Wisconsin (Diagnostic) — Kaggle*

| Atributo | Detalle |
| :--- | :--- |
| **Formato** | CSV |
| **Contenido** | 30 variables numéricas extraídas de imágenes de biopsia (radio, textura, perímetro, área, etc.) más la variable objetivo: diagnóstico (Maligno/Benigno). |
| **Uso en el proyecto** | Clasificación supervisada (M/B), análisis de correlación entre variables morfológicas y base para algoritmos de clustering. |
| **Flujo de capas** | `Raw` → `Cleanse` (estandarización, eliminación de nulls) → `Curated` (features para ML). |
| **Descarga** | [Breast Cancer Wisconsin (Diagnostic) — Kaggle](https://www.kaggle.com/datasets/uciml/breast-cancer-wisconsin-data) |

---

## 2.2 Dataset 2 — Datos genómicos (Parquet)
**Fuente:** *Africa Synth Cancer — Breast Cancer Genomics SSA — HuggingFace*

| Atributo | Detalle |
| :--- | :--- |
| **Formato** | Parquet (tabular + texto) |
| **Contenido** | 100,000 registros sintéticos con variables de expresión génica, edad al diagnóstico, país y población; orientado a tareas de clasificación y regresión. |
| **Uso en el proyecto** | Análisis de expresión génica, clustering por subpoblaciones, detección de anomalías genómicas y análisis de equidad geográfica. |
| **Flujo de capas** | `Raw` (Parquet nativo) → `Cleanse` (normalización, codificación de variables categóricas) → `Curated`. |
| **Descarga** | [Africa Synth Cancer — Breast Cancer Genomics SSA — HuggingFace](https://huggingface.co/datasets/africansynthcancer/breast-cancer-genomics-ssa) |

---

## 2.3 Dataset 3 — Imágenes de ultrasonido (No estructurado)
**Fuente:** *Breast Ultrasound Images Dataset — Kaggle*

| Atributo | Detalle |
| :--- | :--- |
| **Formato** | PNG / JPG (imágenes médicas) |
| **Contenido** | Imágenes de ultrasonido mamario clasificadas en tres categorías: normal, benigno y maligno. |
| **Uso en el proyecto** | Extracción de metadatos (tamaño, formato, clase), análisis estadístico de distribución y generación de embeddings para clustering. |
| **Flujo de capas** | `Raw` (imágenes originales) → `Cleanse` (metadatos en Parquet, normalización de tamaños) → `Curated` (features extraídas). |
| **Descarga** | [Breast Ultrasound Images Dataset — Kaggle](https://www.kaggle.com/datasets/aryashah482/breast-ultrasound-images) |
