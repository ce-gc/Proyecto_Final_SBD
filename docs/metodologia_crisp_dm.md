# Metodología: CRISP-DM

## 1. Selección de Metodología

Se adopta **CRISP-DM** (*Cross-Industry Standard Process for Data Mining*) como metodología de referencia para este proyecto. Es el estándar más extendido en proyectos de ciencia de datos y cubre de forma natural todas las fases contempladas en la rúbrica: desde la comprensión del negocio hasta el despliegue de resultados.

---

## 2. Justificación de la Elección

CRISP-DM se adapta especialmente bien a este proyecto por las siguientes razones:

- **Heterogeneidad de datos**: soporta múltiples tipos de datos (tabular, genómico, imágenes) dentro de una misma iteración.
- **Carácter iterativo**: permite refinar el análisis de forma progresiva a medida que se comprende mejor la naturaleza de los datos.
- **Separación clara de fases**: distingue entre comprensión del negocio, preparación de datos y modelado, lo que se traslada directamente a la arquitectura de capas del Data Lake.
- **Aplicabilidad en bioinformática**: es ampliamente aceptado en proyectos médicos y de bioinformática, donde la trazabilidad y la reproducibilidad son requisitos críticos.

---

## 3. Adaptación de CRISP-DM al Proyecto

La siguiente tabla detalla cómo cada fase de CRISP-DM se concreta en el contexto de este proyecto:

| # | Fase CRISP-DM | Objetivo en este proyecto | Entregable |
|---|---|---|---|
| 1 | **Business Understanding** | Definir qué patrones clínicos y genómicos se quieren detectar en el cáncer de mama | Preguntas de investigación documentadas |
| 2 | **Data Understanding** | Explorar los tres datasets: distribución de variables, calidad, valores nulos y clases | Informe EDA por dataset |
| 3 | **Data Preparation** | Ingestar, limpiar y transformar los datos; construir las capas Raw, Cleanse y Curated | Capas persistidas en Parquet particionado |
| 4 | **Modeling** | Aplicar técnicas de ML (clasificación, clustering, detección de anomalías) sobre la capa Curated | Modelos entrenados con métricas |
| 5 | **Evaluation** | Interpretar los resultados clínicamente; validar la coherencia entre fuentes | Informe de resultados con visualizaciones |
| 6 | **Deployment** | Exponer los resultados en dashboards y gráficos interpretados | Dashboard final en Power BI / matplotlib |
