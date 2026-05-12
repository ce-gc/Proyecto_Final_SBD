"""
04_limpieza_clinical.py
=======================

Uso (dentro del contenedor):
  spark-submit jobs/04_limpieza_clinical.py
  python jobs/04_limpieza_clinical.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_PARQUET_PATH = os.path.join(DATALAKE_ROOT, "raw", "clinical", "breast-cancer.parquet")
CLEANSE_OUTPUT_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Columnas del dataset clínico
EXPECTED_ID_COL = "id"
EXPECTED_TARGET_COL = "diagnosis"
VALID_DIAGNOSIS_VALUES = {"M", "B"}

EXPECTED_NUMERIC_COLS = [
    "radius_mean", "texture_mean", "perimeter_mean", "area_mean",
    "smoothness_mean", "compactness_mean", "concavity_mean",
    "concave points_mean", "symmetry_mean", "fractal_dimension_mean",
    "radius_se", "texture_se", "perimeter_se", "area_se",
    "smoothness_se", "compactness_se", "concavity_se",
    "concave points_se", "symmetry_se", "fractal_dimension_se",
    "radius_worst", "texture_worst", "perimeter_worst", "area_worst",
    "smoothness_worst", "compactness_worst", "concavity_worst",
    "concave points_worst", "symmetry_worst", "fractal_dimension_worst",
]

# Factor IQR para detección de outliers
IQR_FACTOR = 1.5


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configura logging a consola y a archivo."""
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(
        LOG_DIR,
        f"limpieza_clinical_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("limpieza_clinical")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Funciones de limpieza
# ---------------------------------------------------------------------------

def create_spark_session():
    """Crea y devuelve una SparkSession local."""
    return (
        SparkSession.builder
        .appName("Limpieza Clinical Data")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def drop_empty_columns(df, logger):
    """Elimina columnas que estén completamente vacías (todos nulos)."""
    total_rows = df.count()
    dropped = []
    for col_name in df.columns:
        n_nulls = df.filter(F.col(col_name).isNull()).count()
        if n_nulls == total_rows:
            df = df.drop(col_name)
            dropped.append(col_name)

    if dropped:
        logger.warning("Columnas eliminadas (100%% nulas): %s", dropped)
    else:
        logger.info("No se encontraron columnas completamente vacías.")

    return df, dropped


def deduplicate(df, logger):
    """Elimina duplicados por la columna ID."""
    before = df.count()
    if EXPECTED_ID_COL in df.columns:
        df = df.dropDuplicates([EXPECTED_ID_COL])
        key = EXPECTED_ID_COL
    else:
        df = df.dropDuplicates()
        key = "todas las columnas"

    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning(
            "Deduplicación por '%s': %d duplicados eliminados (%d → %d).",
            key, removed, before, after,
        )
    else:
        logger.info("No se encontraron duplicados (clave: '%s').", key)

    return df, removed


def normalize_diagnosis(df, logger):
    """
    Normaliza la columna diagnosis:
      - Trim + uppercase.
      - Valores fuera de {M, B} → null.
    Devuelve (df, n_invalid).
    """
    if EXPECTED_TARGET_COL not in df.columns:
        logger.warning("Columna '%s' no encontrada. Se omite normalización.", EXPECTED_TARGET_COL)
        return df, 0

    df = df.withColumn(
        EXPECTED_TARGET_COL,
        F.upper(F.trim(F.col(EXPECTED_TARGET_COL)))
    )

    # Contar valores no válidos antes de limpiar
    n_invalid = df.filter(
        ~F.col(EXPECTED_TARGET_COL).isin(list(VALID_DIAGNOSIS_VALUES))
        & F.col(EXPECTED_TARGET_COL).isNotNull()
    ).count()

    if n_invalid > 0:
        logger.warning(
            "%d registros con diagnosis inválido → se marcan como null.", n_invalid,
        )

    # Reemplazar valores inválidos por null
    df = df.withColumn(
        EXPECTED_TARGET_COL,
        F.when(
            F.col(EXPECTED_TARGET_COL).isin(list(VALID_DIAGNOSIS_VALUES)),
            F.col(EXPECTED_TARGET_COL),
        ).otherwise(F.lit(None).cast(StringType()))
    )

    return df, n_invalid


def cast_numeric_columns(df, logger):
    """Asegura que las columnas numéricas esperadas sean DoubleType."""
    casted = []
    for col_name in EXPECTED_NUMERIC_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            casted.append(col_name)

    logger.info("Cast a DoubleType aplicado a %d columnas numéricas.", len(casted))
    return df


def impute_nulls_with_median(df, logger):
    """
    Imputa valores nulos en columnas numéricas con la mediana de cada columna.
    Devuelve (df, dict con columnas imputadas y su mediana).
    """
    imputed = {}
    for col_name in EXPECTED_NUMERIC_COLS:
        if col_name not in df.columns:
            continue

        n_nulls = df.filter(F.col(col_name).isNull()).count()
        if n_nulls == 0:
            continue

        # Calcular mediana (percentil 50) — approxQuantile es eficiente
        median_val = df.stat.approxQuantile(col_name, [0.5], 0.01)
        if median_val and len(median_val) > 0:
            med = float(median_val[0])
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(med)).otherwise(F.col(col_name))
            )
            imputed[col_name] = {"nulls": n_nulls, "median": med}
            logger.info(
                "Columna '%s': %d nulos imputados con mediana=%.4f.",
                col_name, n_nulls, med,
            )

    if not imputed:
        logger.info("No se encontraron nulos en columnas numéricas.")

    return df, imputed


def clamp_outliers_iqr(df, logger):
    """
    Detecta outliers mediante IQR y los clampea a los límites [Q1-1.5*IQR, Q3+1.5*IQR].
    Devuelve (df, dict con estadísticas de outliers por columna).
    """
    outlier_stats = {}

    for col_name in EXPECTED_NUMERIC_COLS:
        if col_name not in df.columns:
            continue

        # Calcular Q1 y Q3
        quantiles = df.stat.approxQuantile(col_name, [0.25, 0.75], 0.01)
        if not quantiles or len(quantiles) < 2:
            continue

        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1

        if iqr == 0:
            continue

        lower_bound = q1 - IQR_FACTOR * iqr
        upper_bound = q3 + IQR_FACTOR * iqr

        # Contar outliers antes de clampear
        n_outliers = df.filter(
            (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound)
        ).count()

        if n_outliers > 0:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < lower_bound, F.lit(lower_bound))
                .when(F.col(col_name) > upper_bound, F.lit(upper_bound))
                .otherwise(F.col(col_name))
            )
            outlier_stats[col_name] = {
                "n_outliers": n_outliers,
                "lower_bound": round(lower_bound, 4),
                "upper_bound": round(upper_bound, 4),
            }
            logger.debug(
                "Columna '%s': %d outliers clampeados [%.4f, %.4f].",
                col_name, n_outliers, lower_bound, upper_bound,
            )

    total_outliers = sum(s["n_outliers"] for s in outlier_stats.values())
    logger.info(
        "Outliers (IQR×%.1f): %d valores clampeados en %d columnas.",
        IQR_FACTOR, total_outliers, len(outlier_stats),
    )
    return df, outlier_stats


def drop_rows_without_diagnosis(df, logger):
    """Elimina filas cuyo diagnosis sea null (no recuperable)."""
    if EXPECTED_TARGET_COL not in df.columns:
        return df, 0

    before = df.count()
    df = df.filter(F.col(EXPECTED_TARGET_COL).isNotNull())
    after = df.count()
    removed = before - after

    if removed > 0:
        logger.warning(
            "%d filas eliminadas por diagnosis nulo (%d → %d).",
            removed, before, after,
        )
    else:
        logger.info("Todos los registros tienen diagnosis válido.")

    return df, removed


def add_audit_columns(df):
    """Añade columnas de auditoría para trazabilidad."""
    return (
        df
        .withColumn("_clean_ts", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_clean_job", F.lit("04_limpieza_clinical"))
    )


def compute_quality_summary(df, logger):
    """Calcula métricas de calidad finales del DataFrame limpio."""
    total_rows = df.count()
    logger.info("Total de registros tras limpieza: %d", total_rows)

    if total_rows == 0:
        return {"total_rows": 0, "null_counts": {}, "diagnosis_distribution": {}}

    # Nulos residuales
    null_counts = {}
    check_cols = [EXPECTED_ID_COL, EXPECTED_TARGET_COL] + EXPECTED_NUMERIC_COLS
    for col_name in check_cols:
        if col_name in df.columns:
            n = df.filter(F.col(col_name).isNull()).count()
            if n > 0:
                null_counts[col_name] = n

    # Distribución de diagnosis
    diag_dist = {}
    if EXPECTED_TARGET_COL in df.columns:
        rows = df.groupBy(EXPECTED_TARGET_COL).count().collect()
        diag_dist = {row[EXPECTED_TARGET_COL]: row["count"] for row in rows}
        logger.info("Distribución de diagnosis: %s", diag_dist)

    return {
        "total_rows": total_rows,
        "null_counts": null_counts,
        "diagnosis_distribution": diag_dist,
    }


def persist_clean_parquet(df, output_path, logger):
    """
    Persiste el DataFrame limpio como Parquet particionado por diagnosis.
    Idempotente: si ya existe, lo sobreescribe para reflejar la última limpieza.
    """
    logger.info("Escribiendo Parquet limpio particionado en '%s'...", output_path)
    os.makedirs(output_path, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").partitionBy(EXPECTED_TARGET_COL).parquet(output_path)
    logger.info("Parquet limpio escrito correctamente.")
    return True


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecución."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"limpieza_clinical_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecución guardado en '%s'.", log_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Limpieza del dataset clínico")
    logger.info("=" * 60)
    logger.info("DATALAKE_ROOT: %s", DATALAKE_ROOT)

    execution_log = {
        "job": "04_limpieza_clinical",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "input_path": RAW_PARQUET_PATH,
        "output_path": CLEANSE_OUTPUT_PATH,
    }

    spark = None
    try:
        # ----- 1. Verificar que el Parquet de entrada existe -----
        if not os.path.exists(RAW_PARQUET_PATH):
            msg = f"El Parquet raw no existe: {RAW_PARQUET_PATH}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        logger.info("Parquet de entrada: %s", RAW_PARQUET_PATH)

        # ----- 2. Crear SparkSession -----
        spark = create_spark_session()
        logger.info("SparkSession creada correctamente.")

        # ----- 3. Leer datos raw -----
        df = spark.read.parquet(RAW_PARQUET_PATH)
        initial_rows = df.count()
        initial_cols = len(df.columns)
        logger.info("Datos raw leídos: %d filas × %d columnas.", initial_rows, initial_cols)
        execution_log["initial_rows"] = initial_rows
        execution_log["initial_cols"] = initial_cols

        # ----- 4. Eliminar columnas vacías -----
        df, dropped_cols = drop_empty_columns(df, logger)
        execution_log["dropped_empty_cols"] = dropped_cols

        # ----- 5. Deduplicación por ID -----
        df, n_dupes = deduplicate(df, logger)
        execution_log["duplicates_removed"] = n_dupes

        # ----- 6. Normalización de diagnosis -----
        df, n_invalid_diag = normalize_diagnosis(df, logger)
        execution_log["invalid_diagnosis"] = n_invalid_diag

        # ----- 7. Cast de columnas numéricas -----
        df = cast_numeric_columns(df, logger)

        # ----- 8. Imputación de nulos con mediana -----
        df, imputation_report = impute_nulls_with_median(df, logger)
        execution_log["imputation"] = imputation_report

        # ----- 9. Clamp de outliers (IQR) -----
        df, outlier_report = clamp_outliers_iqr(df, logger)
        execution_log["outliers"] = outlier_report

        # ----- 10. Eliminar filas sin diagnosis -----
        df, n_dropped_no_diag = drop_rows_without_diagnosis(df, logger)
        execution_log["rows_dropped_no_diagnosis"] = n_dropped_no_diag

        # ----- 11. Añadir columnas de auditoría -----
        df = add_audit_columns(df)
        logger.info("Columnas de auditoría añadidas (_clean_ts, _clean_job).")

        # ----- 12. Resumen de calidad final -----
        quality = compute_quality_summary(df, logger)
        execution_log["quality_final"] = quality

        # ----- 13. Persistir en cleanse/clinical -----
        persist_clean_parquet(df, CLEANSE_OUTPUT_PATH, logger)

        # ----- 14. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["final_rows"] = quality["total_rows"]
        execution_log["final_cols"] = len(df.columns)

        logger.info("=" * 60)
        logger.info("FIN - Limpieza completada con éxito.")
        logger.info(
            "  Filas: %d → %d | Duplicados: %d | Sin diagnosis: %d | Outliers clampeados: %d",
            initial_rows,
            quality["total_rows"],
            n_dupes,
            n_dropped_no_diag,
            sum(s["n_outliers"] for s in outlier_report.values()),
        )
        logger.info("  Resultado en: %s", CLEANSE_OUTPUT_PATH)
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la limpieza: %s", e)
        sys.exit(1)

    finally:
        write_execution_log(execution_log, logger)
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
