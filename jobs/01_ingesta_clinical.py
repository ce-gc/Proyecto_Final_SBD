"""
01_ingesta_clinical.py
======================

Uso (dentro del contenedor):
  spark-submit jobs/01_ingesta_clinical.py
  python jobs/01_ingesta_clinical.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType, StringType

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

# Rutas: el datalake se monta en /datalake dentro del contenedor.
# Si se ejecuta fuera de Docker, se usa la ruta relativa al proyecto.
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_CSV_PATH = os.path.join(DATALAKE_ROOT, "raw", "clinical", "breast-cancer.csv")
RAW_PARQUET_PATH = os.path.join(DATALAKE_ROOT, "raw", "clinical", "breast-cancer.parquet")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Columnas esperadas: id + diagnosis + 30 features numericas
EXPECTED_ID_COL = "id"
EXPECTED_TARGET_COL = "diagnosis"

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

ALL_EXPECTED_COLS = [EXPECTED_ID_COL, EXPECTED_TARGET_COL] + EXPECTED_NUMERIC_COLS


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configura logging a consola y a archivo."""
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(
        LOG_DIR,
        f"ingesta_clinical_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("ingesta_clinical")
    logger.setLevel(logging.DEBUG)

    # Evitar duplicar handlers si se ejecuta varias veces en la misma sesion
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
        .appName("Ingesta Clinical CSV")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def validate_schema(df, logger):
    """
    Valida el schema del DataFrame contra las columnas esperadas.

    Returns:
        dict con claves: missing_cols, extra_cols, type_errors
    Raises:
        ValueError si faltan columnas esperadas.
    """
    actual_cols = set(df.columns)
    expected_set = set(ALL_EXPECTED_COLS)

    missing_cols = expected_set - actual_cols
    extra_cols = actual_cols - expected_set

    # Registrar columnas extra (se ignoran, no bloquean)
    if extra_cols:
        logger.warning(
            "Columnas inesperadas encontradas (se ignoran): %s",
            sorted(extra_cols),
        )

    # Lanzar excepcion si faltan columnas criticas
    if missing_cols:
        msg = f"Faltan columnas esperadas en el CSV: {sorted(missing_cols)}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        "Todas las columnas esperadas estan presentes (%d columnas).",
        len(expected_set),
    )

    # Verificar tipos numericos
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    type_errors = []
    for col_name in EXPECTED_NUMERIC_COLS:
        col_type = schema_dict.get(col_name)
        if col_type and not isinstance(col_type, NumericType):
            type_errors.append((col_name, str(col_type)))
            logger.warning(
                "Tipo inesperado en columna '%s': esperado numerico, encontrado %s",
                col_name,
                col_type,
            )

    if not type_errors:
        logger.info("Todos los tipos de las columnas numericas son correctos.")
    else:
        logger.warning(
            "Se encontraron %d columnas con tipo no numerico.", len(type_errors)
        )

    # Verificar que diagnosis es string
    diag_type = schema_dict.get(EXPECTED_TARGET_COL)
    if diag_type and not isinstance(diag_type, StringType):
        logger.warning(
            "Tipo inesperado en '%s': esperado StringType, encontrado %s",
            EXPECTED_TARGET_COL,
            diag_type,
        )

    return {
        "missing_cols": sorted(missing_cols),
        "extra_cols": sorted(extra_cols),
        "type_errors": type_errors,
    }


def check_data_quality(df, logger):
    """
    Comprueba la calidad basica de los datos:
    - Conteo de filas
    - Valores nulos por columna
    - Valores unicos de diagnosis
    """
    total_rows = df.count()
    logger.info("Total de registros leidos: %d", total_rows)

    # Conteo de nulls por columna esperada
    null_counts = {}
    for col_name in ALL_EXPECTED_COLS:
        if col_name in df.columns:
            n_nulls = df.filter(df[col_name].isNull()).count()
            if n_nulls > 0:
                null_counts[col_name] = n_nulls
                logger.warning(
                    "Columna '%s' tiene %d valores nulos (%.1f%%).",
                    col_name,
                    n_nulls,
                    (n_nulls / total_rows) * 100,
                )

    if not null_counts:
        logger.info("No se encontraron valores nulos en las columnas esperadas.")

    # Valores unicos de diagnosis
    diag_values = [
        row[EXPECTED_TARGET_COL]
        for row in df.select(EXPECTED_TARGET_COL).distinct().collect()
    ]
    logger.info("Valores unicos de '%s': %s", EXPECTED_TARGET_COL, diag_values)

    return {
        "total_rows": total_rows,
        "null_counts": null_counts,
        "diagnosis_values": diag_values,
    }


def persist_parquet(df, output_path, logger):
    """
    Persiste el DataFrame como Parquet.
    Es idempotente: si el directorio ya existe, no sobreescribe.
    """
    if os.path.exists(output_path):
        logger.info(
            "El snapshot Parquet ya existe en '%s'. Se omite (idempotencia).",
            output_path,
        )
        return False

    logger.info("Escribiendo snapshot Parquet en '%s'...", output_path)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Snapshot Parquet escrito correctamente.")
    return True


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecucion."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"ingesta_clinical_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecucion guardado en '%s'.", log_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Ingesta del dataset clinico (CSV)")
    logger.info("=" * 60)
    logger.info("DATALAKE_ROOT: %s", DATALAKE_ROOT)

    execution_log = {
        "job": "01_ingesta_clinical",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "csv_path": RAW_CSV_PATH,
        "parquet_path": RAW_PARQUET_PATH,
    }

    spark = None
    try:
        # ----- 1. Verificar que el CSV de entrada existe -----
        if not os.path.isfile(RAW_CSV_PATH):
            msg = f"El CSV de entrada no existe: {RAW_CSV_PATH}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        logger.info("CSV de entrada: %s", RAW_CSV_PATH)

        # ----- 2. Crear SparkSession -----
        spark = create_spark_session()
        logger.info("SparkSession creada correctamente.")

        # ----- 3. Leer CSV con PySpark -----
        logger.info("Leyendo CSV con inferSchema...")
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(RAW_CSV_PATH)
        )
        logger.info("Schema inferido:")
        df.printSchema()

        # ----- 4. Validar schema -----
        schema_report = validate_schema(df, logger)
        execution_log["schema_validation"] = schema_report

        # ----- 5. Comprobar calidad de datos -----
        quality_report = check_data_quality(df, logger)
        execution_log["data_quality"] = quality_report

        # ----- 6. Seleccionar solo las columnas esperadas -----
        df_clean = df.select(ALL_EXPECTED_COLS)
        logger.info(
            "DataFrame reducido a %d columnas esperadas.",
            len(ALL_EXPECTED_COLS),
        )

        # ----- 7. Persistir snapshot Parquet (idempotente) -----
        was_written = persist_parquet(df_clean, RAW_PARQUET_PATH, logger)
        execution_log["parquet_written"] = was_written

        # ----- 8. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["records_ingested"] = quality_report["total_rows"]
        execution_log["errors"] = 0

        logger.info("=" * 60)
        logger.info("FIN - Ingesta completada con exito.")
        logger.info(
            "  Registros: %d | Errores: 0 | Parquet escrito: %s",
            quality_report["total_rows"],
            was_written,
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta: %s", e)
        sys.exit(1)

    finally:
        # Escribir log de ejecucion
        write_execution_log(execution_log, logger)

        # Cerrar Spark
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
