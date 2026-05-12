"""
06_limpieza_images.py
=====================
Uso (dentro del contenedor):
  spark-submit jobs/06_limpieza_images.py
  python jobs/06_limpieza_images.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_IMAGES_META_PATH = os.path.join(DATALAKE_ROOT, "raw", "images_meta")
CLEANSE_OUTPUT_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "images")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

PARTITION_COL = "category"
VALID_CATEGORIES = {"normal", "benign", "malignant"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"limpieza_images_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("limpieza_images")
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
        .appName("Limpieza Images Meta")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def deduplicate_exact(df, logger):
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Eliminados %d duplicados exactos.", removed)
    else:
        logger.info("No se encontraron duplicados exactos.")
    return df, removed

def deduplicate_by_hash(df, logger):
    if "file_hash" not in df.columns:
        return df, 0
    
    before = df.count()
    # Mantener el primer registro para cada hash
    window_spec = Window.partitionBy("file_hash").orderBy("path")
    df = df.withColumn("rn", F.row_number().over(window_spec))
    df = df.filter(F.col("rn") == 1).drop("rn")
    
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Eliminados %d duplicados por 'file_hash'.", removed)
    else:
        logger.info("No se encontraron duplicados por hash.")
    return df, removed

def filter_invalid_dimensions(df, logger):
    before = df.count()
    df = df.filter((F.col("width") > 0) & (F.col("height") > 0))
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Eliminados %d registros con dimensiones inválidas.", removed)
    return df, removed

def filter_categories(df, logger):
    before = df.count()
    df = df.filter(F.lower(F.col("category")).isin(VALID_CATEGORIES))
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Eliminados %d registros de categorías desconocidas.", removed)
    return df, removed

def add_audit_columns(df):
    return (
        df
        .withColumn("_clean_ts", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_clean_job", F.lit("06_limpieza_images"))
    )

def persist_clean_parquet(df, output_path, logger):
    logger.info("Escribiendo Parquet limpio particionado por '%s' en '%s'...", PARTITION_COL, output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
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
        f"limpieza_images_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
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
    logger.info("INICIO - Limpieza de metadatos de imágenes")
    logger.info("=" * 60)

    execution_log = {
        "job": "06_limpieza_images",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "input_path": RAW_IMAGES_META_PATH,
        "output_path": CLEANSE_OUTPUT_PATH,
    }

    spark = None
    try:
        if not os.path.exists(RAW_IMAGES_META_PATH):
            raise FileNotFoundError(f"No se encuentran los metadatos raw: {RAW_IMAGES_META_PATH}")

        spark = create_spark_session()
        logger.info("SparkSession creada.")

        df = spark.read.parquet(RAW_IMAGES_META_PATH)
        initial_rows = df.count()
        logger.info("Datos raw leídos: %d filas.", initial_rows)
        execution_log["initial_rows"] = initial_rows

        if initial_rows == 0:
            logger.warning("El DataFrame de entrada está vacío.")
            execution_log["status"] = "SUCCESS"
            execution_log["final_rows"] = 0
            return

        df, n_dupes_exact = deduplicate_exact(df, logger)
        df, n_dupes_hash = deduplicate_by_hash(df, logger)
        df, n_invalid_dim = filter_invalid_dimensions(df, logger)
        df, n_invalid_cat = filter_categories(df, logger)
        
        df = add_audit_columns(df)

        final_rows = df.count()
        execution_log["quality_final"] = {"total_rows": final_rows}
        execution_log["dropped_by_hash"] = n_dupes_hash
        execution_log["dropped_by_dimensions"] = n_invalid_dim
        execution_log["dropped_by_category"] = n_invalid_cat

        persist_clean_parquet(df, CLEANSE_OUTPUT_PATH, logger)

        execution_log["status"] = "SUCCESS"
        execution_log["final_rows"] = final_rows

        logger.info("=" * 60)
        logger.info("FIN - Limpieza completada con éxito.")
        logger.info("  Filas: %d → %d", initial_rows, final_rows)
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
