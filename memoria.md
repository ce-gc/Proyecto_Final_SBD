# Memoria Técnica del Proyecto

## Registro de Actividades

### Día 1: Fase 1 — Planificación y metodología
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
├── tests/
```

### Día 2: Fase 2 — Ingesta de datos (Raw Layer)
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

**Resultados de la ejecución (validados en contenedor Docker):**

| Job | Registros | Columnas | Errores | Resultado |
|-----|-----------|----------|---------|-----------|
| `01_ingesta_clinical.py` | 569 filas | 32 (id + diagnosis + 30 numéricas) | 0 | Parquet generado en `raw/clinical/` |
| `02_ingesta_genomics.py` | 100,000 filas | 130 | 0 (7 columnas con nulls detectados) | Fichero original preservado |
| `03_ingesta_images.py` | 1,578 imágenes | 7 metadatos por imagen | 0 corruptas | Parquet generado en `raw/images_meta/` |

**Detalle de nulls detectados en dataset genómico (informativo, no bloqueante):**
- `age_at_diagnosis`: 497 nulls (0.5%)
- `molecular_subtype`: 974 nulls (1.0%)
- `survival_months`: 10,252 nulls (10.3%)
- `vital_status`: 4,901 nulls (4.9%)
- `recurrence_free_months`: 12,159 nulls (12.2%)
- `recurrence_event`: 7,939 nulls (7.9%)
- `distant_metastasis`: 11,770 nulls (11.8%)

**Distribución de imágenes por categoría:**
- Benign: 891 imágenes
- Malignant: 421 imágenes
- Normal: 266 imágenes

---
*(Este archivo se continuará actualizando con las siguientes fases del proyecto.)*
