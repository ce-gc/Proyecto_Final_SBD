"""
05_limpieza_genomics.py
=======================

Uso (dentro del contenedor):
  spark-submit jobs/05_limpieza_genomics.py
  python jobs/05_limpieza_genomics.py
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

RAW_GENOMICS_PATH = os.path.join(
    DATALAKE_ROOT, "raw", "genomics", "breast_cancer_expression_ssa_synthetic.parquet"
)
CLEANSE_OUTPUT_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

PARTITION_COL = "molecular_subtype"

CATEGORICAL_COLS = [
    "population", "country", "batch", "vital_status", "recurrence_event", "molecular_subtype"
]
NUMERIC_COLS = [
    "age_at_diagnosis", "survival_months", "recurrence_free_months"
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"limpieza_genomics_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("limpieza_genomics")
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
    return (
        SparkSession.builder
        .appName("Limpieza Genomics Data")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def drop_empty_columns(df, logger):
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
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Deduplicación: %d duplicados eliminados (%d → %d).", removed, before, after)
    else:
        logger.info("No se encontraron duplicados.")
    return df, removed

def clean_categorical_columns(df, logger):
    for col_name in CATEGORICAL_COLS:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.upper(F.trim(F.col(col_name)))
            )
            # Rellenar nulos con UNKNOWN
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), "UNKNOWN").otherwise(F.col(col_name))
            )
    logger.info("Columnas categóricas limpiadas y nulos rellenados con 'UNKNOWN'.")
    return df

def cast_and_impute_numeric_columns(df, logger):
    for col_name in NUMERIC_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            
            n_nulls = df.filter(F.col(col_name).isNull()).count()
            if n_nulls > 0:
                median_val = df.stat.approxQuantile(col_name, [0.5], 0.01)
                if median_val and len(median_val) > 0:
                    med = float(median_val[0])
                    df = df.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), F.lit(med)).otherwise(F.col(col_name))
                    )
                    logger.info("Columna '%s': %d nulos imputados con mediana=%.4f.", col_name, n_nulls, med)
    return df

def add_audit_columns(df):
    return (
        df
        .withColumn("_clean_ts", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_clean_job", F.lit("05_limpieza_genomics"))
    )

def compute_quality_summary(df, logger):
    total_rows = df.count()
    logger.info("Total de registros tras limpieza: %d", total_rows)
    return {"total_rows": total_rows}

def persist_clean_parquet(df, output_path, logger):
    logger.info("Escribiendo Parquet limpio particionado por '%s' en '%s'...", PARTITION_COL, output_path)
    os.makedirs(output_path, exist_ok=True)
    if PARTITION_COL in df.columns:
        df.coalesce(1).write.mode("overwrite").partitionBy(PARTITION_COL).parquet(output_path)
    else:
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Parquet limpio escrito correctamente.")
    return True

def write_execution_log(log_data, logger):
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"limpieza_genomics_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
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
    logger.info("INICIO - Limpieza del dataset genómico")
    logger.info("=" * 60)

    execution_log = {
        "job": "05_limpieza_genomics",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "input_path": RAW_GENOMICS_PATH,
        "output_path": CLEANSE_OUTPUT_PATH,
    }

    spark = None
    try:
        if not os.path.exists(RAW_GENOMICS_PATH):
            raise FileNotFoundError(f"El Parquet raw no existe: {RAW_GENOMICS_PATH}")

        spark = create_spark_session()
        logger.info("SparkSession creada.")

        df = spark.read.parquet(RAW_GENOMICS_PATH)
        initial_rows = df.count()
        logger.info("Datos raw leídos: %d filas.", initial_rows)
        execution_log["initial_rows"] = initial_rows

        df, dropped_cols = drop_empty_columns(df, logger)
        df, n_dupes = deduplicate(df, logger)
        df = clean_categorical_columns(df, logger)
        df = cast_and_impute_numeric_columns(df, logger)
        df = add_audit_columns(df)

        quality = compute_quality_summary(df, logger)
        execution_log["quality_final"] = quality

        persist_clean_parquet(df, CLEANSE_OUTPUT_PATH, logger)

        execution_log["status"] = "SUCCESS"
        execution_log["final_rows"] = quality["total_rows"]

        logger.info("=" * 60)
        logger.info("FIN - Limpieza completada con éxito.")
        logger.info("  Filas: %d → %d", initial_rows, quality["total_rows"])
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
