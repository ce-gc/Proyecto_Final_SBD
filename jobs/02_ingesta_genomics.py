"""
02_ingesta_genomics.py
======================

Uso (dentro del contenedor):
  spark-submit jobs/02_ingesta_genomics.py
  python jobs/02_ingesta_genomics.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "raw", "genomics", "breast_cancer_expression_ssa_synthetic.parquet")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Schema de referencia: columnas criticas que DEBEN estar presentes
CRITICAL_COLS = [
    "population", "country", "age_at_diagnosis", "batch",
    "molecular_subtype", "survival_months", "vital_status",
    "recurrence_free_months", "recurrence_event", "distant_metastasis",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configura logging a consola y a archivo."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"ingesta_genomics_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("ingesta_genomics")
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
# Funciones principales
# ---------------------------------------------------------------------------

def create_spark_session():
    """Crea y devuelve una SparkSession local."""
    return (
        SparkSession.builder
        .appName("Ingesta Genomics Parquet")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def validate_critical_columns(df, logger):
    """
    Valida que todas las columnas criticas esten presentes.
    Registra columnas extra encontradas.
    
    Raises:
        ValueError si faltan columnas criticas.
    """
    actual_cols = set(df.columns)
    critical_set = set(CRITICAL_COLS)

    missing = critical_set - actual_cols
    extra = actual_cols - critical_set

    if extra:
        logger.info(
            "Columnas adicionales encontradas (schema evolution aceptada): %d columnas extra.",
            len(extra),
        )
        logger.debug("Columnas extra: %s", sorted(extra)[:20])

    if missing:
        msg = f"Faltan columnas criticas en el dataset: {sorted(missing)}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        "Validacion de columnas criticas superada (%d/%d presentes).",
        len(critical_set),
        len(critical_set),
    )

    return {
        "missing_critical": sorted(missing),
        "extra_cols_count": len(extra),
    }


def check_data_quality(df, logger):
    """Comprueba calidad basica: conteo de filas, nulls en columnas criticas."""
    total_rows = df.count()
    total_cols = len(df.columns)
    logger.info("Datos cargados: %d filas, %d columnas.", total_rows, total_cols)

    null_counts = {}
    for col_name in CRITICAL_COLS:
        if col_name in df.columns:
            n_nulls = df.filter(df[col_name].isNull()).count()
            if n_nulls > 0:
                null_counts[col_name] = n_nulls
                logger.warning(
                    "Columna critica '%s' tiene %d valores nulos (%.1f%%).",
                    col_name,
                    n_nulls,
                    (n_nulls / total_rows) * 100 if total_rows > 0 else 0,
                )

    if not null_counts:
        logger.info("No se encontraron valores nulos en las columnas criticas.")

    return {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "null_counts": null_counts,
    }


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecucion."""
    os.makedirs(LOG_DIR, exist_ok=True)
    summary_file = os.path.join(
        LOG_DIR,
        f"ingesta_genomics_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecucion guardado en '%s'.", summary_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Ingesta del dataset genomico (Parquet)")
    logger.info("=" * 60)
    logger.info("DATALAKE_ROOT: %s", DATALAKE_ROOT)

    execution_log = {
        "job": "02_ingesta_genomics",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "input_path": RAW_GENOMICS_PATH,
    }

    spark = None
    try:
        # ----- 1. Verificar que el Parquet existe -----
        if not os.path.exists(RAW_GENOMICS_PATH):
            msg = f"No se encuentra el archivo: {RAW_GENOMICS_PATH}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        logger.info("Parquet de entrada: %s", RAW_GENOMICS_PATH)

        # ----- 2. Crear SparkSession -----
        spark = create_spark_session()
        logger.info("SparkSession creada correctamente.")

        # ----- 3. Leer Parquet con PySpark -----
        logger.info("Leyendo Parquet...")
        df = spark.read.option("mergeSchema", "true").parquet(RAW_GENOMICS_PATH)

        logger.info("Schema del Parquet:")
        df.printSchema()

        # ----- 4. Validar columnas criticas -----
        schema_report = validate_critical_columns(df, logger)
        execution_log["schema_validation"] = schema_report

        # ----- 5. Comprobar calidad de datos -----
        quality_report = check_data_quality(df, logger)
        execution_log["data_quality"] = quality_report

        # ----- 6. Resumen final -----
        # El fichero original se mantiene sin modificar en Raw (no se genera copia).
        execution_log["status"] = "SUCCESS"
        execution_log["records_ingested"] = quality_report["total_rows"]
        execution_log["errors"] = 0

        logger.info("=" * 60)
        logger.info("FIN - Ingesta de genomicos completada.")
        logger.info(
            "  Registros: %d | Columnas: %d | Errores: 0",
            quality_report["total_rows"],
            quality_report["total_cols"],
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta de genomicos: %s", e)
        sys.exit(1)

    finally:
        write_execution_log(execution_log, logger)
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
