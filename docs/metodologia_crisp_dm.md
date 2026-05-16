# Metodología: CRISP-DM

## 1. Selección de Metodología

Se adopta **CRISP-DM** (*Cross-Industry Standard Process for Data Mining*) como metodología de referencia para este proyecto. Es el estándar más extendido en proyectos de ciencia de datos y cubre de forma natural todas las fases contempladas en la rúbrica: desde la comprensión del negocio hasta el despliegue de resultados.

---

## 2. Justificación de la Elección

CRISP-DM se ha adoptado en este proyecto no solo por ser el estándar de facto de la industria, sino por su perfecta alineación con los desafíos inherentes a los datos médicos:

- **Enfoque en los objetivos del negocio (Salud Pública)**: A diferencia de metodologías centradas puramente en el software (como Agile/Scrum tradicional) o en la base de datos (ETL clásicos), CRISP-DM comienza entendiendo el problema clínico: la necesidad de diagnosticar, segmentar y comprender el cáncer de mama.
- **Gestión de la Heterogeneidad**: Los datos de salud rara vez provienen de una única fuente. CRISP-DM proporciona un marco lo suficientemente flexible para acomodar datos estructurados (CSV clínico), semiestructurados/Big Data (Parquet genómico) y no estructurados (imágenes de ultrasonido) dentro de un mismo ciclo de procesamiento.
- **Carácter Iterativo y Refinamiento (El "Bucle" de CRISP-DM)**: El análisis de datos médicos requiere descubrimiento. Durante la fase de *Data Understanding*, es común detectar inconsistencias que obligan a volver a la *Business Understanding* o revisar la ingesta. CRISP-DM naturaliza este proceso de retroceso constructivo.
- **Trazabilidad y Reproducibilidad**: En el entorno médico/científico, cada decisión algorítmica debe ser auditable. Las fases claras de CRISP-DM obligan a documentar qué se hizo en la preparación (Cleanse) y qué se modeló (Curated), garantizando que los hallazgos finales puedan ser explicados.

---

## 3. Adaptación de CRISP-DM al Proyecto

La siguiente tabla detalla cómo el ciclo iterativo de CRISP-DM ha dirigido arquitectónicamente y analíticamente este proyecto:

| Fase CRISP-DM | Aplicación Práctica en el Proyecto (El "Cómo") | Entregable / Capa Asociada |
| :--- | :--- | :--- |
| **1. Business Understanding** | Definición de los objetivos médicos: predecir la malignidad de tumores, segmentar pacientes según genética para medicina personalizada y analizar el balance de datos para futuros modelos de visión artificial. | Documento de planificación inicial y definición de objetivos analíticos. |
| **2. Data Understanding** | Análisis Exploratorio Inicial: Revisión de distribuciones, conteo de nulos, identificación de tipos de datos anómalos y lectura cruda de los orígenes (Kaggle y HuggingFace). | Scripts de la **Capa Raw** (`01`, `02`, `03`) e informe de estadísticas base. |
| **3. Data Preparation** | Transformación profunda: Imputación de nulos estadísticamente, tratamiento de outliers (clampeo IQR), homogeneización de tipos y particionado físico para optimizar consultas (por diagnóstico o país). | Scripts de la **Capa Cleanse** (`04`, `05`, `06`) guardados en Parquet. |
| **4. Modeling** | Aplicación de algoritmos ML: Extracción de *Feature Importance* con Random Forest, clustering de pacientes con PCA y K-Means, y detección de registros atípicos usando Isolation Forest. | Scripts de la **Capa Curated** (`07`, `08`, `09`) con datasets enriquecidos. |
| **5. Evaluation** | Validación de los modelos: Extracción de métricas de rendimiento (AUC-ROC, Matrices de confusión, Silhouette Scores) y generación de visualizaciones (Elbow plots, correlaciones) para asegurar la validez biológica. | Scripts de métricas (`10`, `11`, `12`) y visualización (`13`, `14`, `15`). |
| **6. Deployment** | Puesta a disposición del usuario final (médicos o analistas) de forma interactiva y sin necesidad de programar, permitiendo explorar los descubrimientos visualmente. | **Dashboard Interactivo** (`16_dashboard.py`) usando Plotly Dash. |

Como se observa, existe una simbiosis directa entre la metodología de ciencia de datos (CRISP-DM) y la ingeniería de datos utilizada (Arquitectura Medallón de Data Lake), donde las fases 2 y 3 construyen la infraestructura que permite ejecutar las fases 4, 5 y 6 con garantías.
