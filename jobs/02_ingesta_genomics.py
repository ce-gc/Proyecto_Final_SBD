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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType, DoubleType, IntegerType, LongType,
)
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_GENOMICS_PATH = os.path.join(
    DATALAKE_ROOT, "raw", "genomics",
    "breast_cancer_expression_ssa_synthetic.parquet",
)
SCHEMA_SNAPSHOT_PATH = os.path.join(
    DATALAKE_ROOT, "raw", "genomics", "_schema_snapshot.json",
)
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Schema de referencia: columnas críticas que DEBEN estar presentes.
# Si faltan, se rellenan con null (no se aborta la ingesta).
CRITICAL_COLS = [
    "population", "country", "age_at_diagnosis", "batch",
    "molecular_subtype", "survival_months", "vital_status",
    "recurrence_free_months", "recurrence_event", "distant_metastasis",
]

# Columnas con restricciones de rango (para validación de calidad)
RANGE_VALIDATIONS = {
    "age_at_diagnosis": (0, 120),
    "survival_months": (0, 600),
    "recurrence_free_months": (0, 600),
}


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
        .config("spark.sql.parquet.mergeSchema", "true")       # Merge global
        .config("spark.sql.files.ignoreCorruptFiles", "true")   # Omitir corruptos
        .getOrCreate()
    )


def read_parquet_safe(spark, path, logger):
    """
    Lee un Parquet con manejo de errores.

    Si la lectura directa falla (fichero corrupto), intenta buscar
    fragmentos .parquet válidos dentro del directorio.
    """
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        logger.info("Parquet leído correctamente: %s", path)
        return df, False
    except AnalysisException as e:
        logger.warning(
            "Error leyendo Parquet directamente: %s. Intentando lectura de fragmentos...",
            str(e)[:200],
        )
    except Exception as e:
        logger.warning(
            "Error inesperado leyendo Parquet: %s. Intentando lectura de fragmentos...",
            str(e)[:200],
        )

    # Fallback: buscar archivos .parquet individuales dentro del directorio
    if os.path.isdir(path):
        fragments = []
        for root, dirs, files in os.walk(path):
            for f in files:
                if f.endswith(".parquet") and not f.startswith("_"):
                    fragments.append(os.path.join(root, f))

        if fragments:
            logger.info(
                "Encontrados %d fragmentos Parquet. Intentando lectura parcial...",
                len(fragments),
            )
            valid_dfs = []
            for frag in fragments:
                try:
                    frag_df = spark.read.parquet(frag)
                    valid_dfs.append(frag_df)
                    logger.debug("Fragmento OK: %s", frag)
                except Exception as frag_e:
                    logger.warning("Fragmento corrupto '%s': %s", frag, frag_e)

            if valid_dfs:
                from functools import reduce
                df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), valid_dfs)
                logger.info(
                    "Lectura parcial exitosa: %d fragmentos de %d.",
                    len(valid_dfs), len(fragments),
                )
                return df, True
            else:
                raise RuntimeError(
                    f"Todos los fragmentos Parquet en '{path}' están corruptos."
                )
        else:
            raise FileNotFoundError(
                f"No se encontraron archivos .parquet dentro de '{path}'."
            )
    else:
        raise RuntimeError(f"No se pudo leer el archivo Parquet: {path}")


def validate_critical_columns(df, logger):
    """
    Valida columnas críticas con tolerancia a schema evolution.

    - Columnas extra: se conservan (INFO).
    - Columnas ausentes: se añaden con null (WARNING, no crash).
    """
    actual_cols = set(df.columns)
    critical_set = set(CRITICAL_COLS)

    missing = sorted(critical_set - actual_cols)
    extra = sorted(actual_cols - critical_set)

    if extra:
        logger.info(
            "Schema evolution: %d columnas adicionales detectadas (se conservan).",
            len(extra),
        )
        logger.debug("Columnas extra (muestra): %s", extra[:20])

    if missing:
        logger.warning(
            "Faltan %d columnas críticas. Se añadirán con null: %s",
            len(missing), missing,
        )
        for col_name in missing:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    logger.info(
        "Validación de columnas críticas: %d/%d presentes, %d rellenadas, %d extra.",
        len(critical_set) - len(missing),
        len(critical_set),
        len(missing),
        len(extra),
    )

    return df, {
        "missing_critical": missing,
        "extra_cols_count": len(extra),
        "total_cols": len(df.columns),
    }


def validate_ranges(df, logger):
    """
    Valida que los valores numéricos estén en rangos razonables.
    Registra outliers pero no los elimina.
    """
    range_report = {}
    for col_name, (lo, hi) in RANGE_VALIDATIONS.items():
        if col_name not in df.columns:
            continue
        try:
            out_of_range = df.filter(
                (F.col(col_name).isNotNull())
                & ((F.col(col_name) < lo) | (F.col(col_name) > hi))
            ).count()
            range_report[col_name] = {
                "expected_range": [lo, hi],
                "out_of_range_count": out_of_range,
            }
            if out_of_range > 0:
                logger.warning(
                    "Columna '%s': %d valores fuera del rango [%s, %s].",
                    col_name, out_of_range, lo, hi,
                )
            else:
                logger.info("Columna '%s': todos los valores en rango [%s, %s].", col_name, lo, hi)
        except Exception as e:
            logger.warning("No se pudo validar rango de '%s': %s", col_name, e)

    return range_report


def deduplicate(df, logger):
    """Elimina duplicados exactos (por todas las columnas)."""
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    removed = before - after
    if removed > 0:
        logger.warning("Deduplicación: %d duplicados eliminados (%d → %d).", removed, before, after)
    else:
        logger.info("No se encontraron duplicados.")
    return df, removed


def check_data_quality(df, logger):
    """Comprueba calidad básica: conteo de filas, nulls en columnas críticas."""
    total_rows = df.count()
    total_cols = len(df.columns)
    logger.info("Datos cargados: %d filas, %d columnas.", total_rows, total_cols)

    if total_rows == 0:
        logger.warning("El DataFrame está vacío.")
        return {"total_rows": 0, "total_cols": total_cols, "null_counts": {}}

    null_counts = {}
    for col_name in CRITICAL_COLS:
        if col_name in df.columns:
            n_nulls = df.filter(df[col_name].isNull()).count()
            if n_nulls > 0:
                null_counts[col_name] = n_nulls
                logger.warning(
                    "Columna crítica '%s' tiene %d valores nulos (%.1f%%).",
                    col_name,
                    n_nulls,
                    (n_nulls / total_rows) * 100 if total_rows > 0 else 0,
                )

    if not null_counts:
        logger.info("No se encontraron valores nulos en las columnas críticas.")

    return {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "null_counts": null_counts,
    }


def save_schema_snapshot(df, path, logger):
    """
    Guarda un snapshot JSON del schema real del DataFrame.
    Útil para detectar schema evolution entre ejecuciones.
    """
    schema_json = df.schema.jsonValue()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema_json, f, indent=2, ensure_ascii=False)
        logger.info("Schema snapshot guardado en '%s'.", path)
    except Exception as e:
        logger.warning("No se pudo guardar el schema snapshot: %s", e)


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecución."""
    os.makedirs(LOG_DIR, exist_ok=True)
    summary_file = os.path.join(
        LOG_DIR,
        f"ingesta_genomics_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecución guardado en '%s'.", summary_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Ingesta del dataset genómico (Parquet)")
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

        # Verificar tamaño (fichero vacío o demasiado pequeño)
        if os.path.isfile(RAW_GENOMICS_PATH):
            fsize = os.path.getsize(RAW_GENOMICS_PATH)
            if fsize == 0:
                msg = f"El archivo Parquet está vacío (0 bytes): {RAW_GENOMICS_PATH}"
                logger.error(msg)
                raise ValueError(msg)
            logger.info("Parquet de entrada: %s (%d bytes)", RAW_GENOMICS_PATH, fsize)
        else:
            logger.info("Parquet de entrada (directorio): %s", RAW_GENOMICS_PATH)

        # ----- 2. Crear SparkSession -----
        spark = create_spark_session()
        logger.info("SparkSession creada correctamente.")

        # ----- 3. Leer Parquet de forma segura -----
        df, had_fallback = read_parquet_safe(spark, RAW_GENOMICS_PATH, logger)
        execution_log["used_fallback_reading"] = had_fallback

        logger.info("Schema del Parquet:")
        df.printSchema()

        # ----- 4. Validar columnas críticas (schema-evolution friendly) -----
        df, schema_report = validate_critical_columns(df, logger)
        execution_log["schema_validation"] = schema_report

        # ----- 5. Validar rangos numéricos -----
        range_report = validate_ranges(df, logger)
        execution_log["range_validation"] = range_report

        # ----- 6. Deduplicación -----
        df, n_dupes = deduplicate(df, logger)
        execution_log["duplicates_removed"] = n_dupes

        # ----- 7. Comprobar calidad de datos -----
        quality_report = check_data_quality(df, logger)
        execution_log["data_quality"] = quality_report

        # ----- 8. Guardar snapshot del schema -----
        save_schema_snapshot(df, SCHEMA_SNAPSHOT_PATH, logger)

        # ----- 9. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["records_ingested"] = quality_report["total_rows"]
        execution_log["errors"] = 0

        logger.info("=" * 60)
        logger.info("FIN - Ingesta de genómicos completada.")
        logger.info(
            "  Registros: %d | Columnas: %d | Duplicados: %d | Errores: 0",
            quality_report["total_rows"],
            quality_report["total_cols"],
            n_dupes,
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta de genómicos: %s", e)
        sys.exit(1)

    finally:
        write_execution_log(execution_log, logger)
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
