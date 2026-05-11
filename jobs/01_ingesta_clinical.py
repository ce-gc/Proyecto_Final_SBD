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
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, NumericType, StringType
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_CSV_PATH = os.path.join(DATALAKE_ROOT, "raw", "clinical", "breast-cancer.csv")
RAW_PARQUET_PATH = os.path.join(DATALAKE_ROOT, "raw", "clinical", "breast-cancer.parquet")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Columnas esperadas: id + diagnosis + 30 features numéricas
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

    # Evitar duplicar handlers si se ejecuta varias veces en la misma sesión
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


def read_csv_safe(spark, path, logger):
    """
    Lee el CSV en modo PERMISSIVE con columna de registros corruptos.

    Modo PERMISSIVE: las filas que no encajan con el schema inferido se
    almacenan enteras en la columna '_corrupt_record' en vez de provocar
    un fallo de lectura.
    """
    logger.info("Leyendo CSV con inferSchema + mode PERMISSIVE...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", True)          # Soporta saltos de línea dentro de campos
        .option("escape", '"')              # Escape estándar CSV
        .option("encoding", "UTF-8")
        .csv(path)
    )
    return df


def handle_corrupt_records(df, logger):
    """
    Detecta y separa las filas marcadas como corruptas por Spark.
    Devuelve (df_clean, n_corrupt).
    """
    n_corrupt = 0
    if "_corrupt_record" in df.columns:
        corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
        n_corrupt = corrupt_df.count()
        if n_corrupt > 0:
            logger.warning(
                "%d filas corruptas detectadas. Ejemplos:", n_corrupt,
            )
            for row in corrupt_df.select("_corrupt_record").limit(5).collect():
                logger.warning("  → %s", row["_corrupt_record"][:200])
            # Eliminar filas corruptas y la columna auxiliar
            df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        else:
            df = df.drop("_corrupt_record")
    return df, n_corrupt


def validate_schema(df, logger):
    """
    Valida el schema del DataFrame contra las columnas esperadas.

    Schema-evolution friendly:
      - Columnas extra: se conservan (no se eliminan). Se loguean como INFO.
      - Columnas ausentes: se añaden con valor null. Se loguean como WARNING.

    Returns:
        tuple(df_augmented, report_dict)
    """
    actual_cols = set(df.columns)
    expected_set = set(ALL_EXPECTED_COLS)

    missing_cols = sorted(expected_set - actual_cols)
    extra_cols = sorted(actual_cols - expected_set)

    # --- Columnas extra (schema evolution) ---
    if extra_cols:
        logger.info(
            "Schema evolution: %d columnas nuevas detectadas (se conservan): %s",
            len(extra_cols),
            extra_cols[:10],
        )

    # --- Columnas ausentes: rellenar con null en vez de crashear ---
    if missing_cols:
        logger.warning(
            "Faltan %d columnas esperadas. Se añadirán con valor null: %s",
            len(missing_cols),
            missing_cols,
        )
        for col_name in missing_cols:
            # Determinar tipo esperado
            if col_name in EXPECTED_NUMERIC_COLS:
                df = df.withColumn(col_name, F.lit(None).cast(DoubleType()))
            elif col_name == EXPECTED_TARGET_COL:
                df = df.withColumn(col_name, F.lit(None).cast(StringType()))
            else:
                df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    logger.info(
        "Validación de columnas: %d esperadas, %d presentes, %d nuevas, %d rellenadas.",
        len(expected_set),
        len(expected_set) - len(missing_cols),
        len(extra_cols),
        len(missing_cols),
    )

    # --- Verificar y corregir tipos numéricos ---
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    type_errors = []
    type_fixes = []
    for col_name in EXPECTED_NUMERIC_COLS:
        if col_name in schema_dict:
            col_type = schema_dict[col_name]
            if not isinstance(col_type, NumericType):
                type_errors.append((col_name, str(col_type)))
                logger.warning(
                    "Tipo inesperado en '%s': %s → intentando cast a DoubleType.",
                    col_name,
                    col_type,
                )
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                type_fixes.append(col_name)

    if type_fixes:
        logger.info("Cast a DoubleType aplicado a: %s", type_fixes)
    elif not type_errors:
        logger.info("Todos los tipos de las columnas numéricas son correctos.")

    # --- Verificar que diagnosis es string ---
    diag_type = schema_dict.get(EXPECTED_TARGET_COL)
    if diag_type and not isinstance(diag_type, StringType):
        logger.warning(
            "Tipo inesperado en '%s': %s → cast a StringType.",
            EXPECTED_TARGET_COL,
            diag_type,
        )
        df = df.withColumn(EXPECTED_TARGET_COL, F.col(EXPECTED_TARGET_COL).cast(StringType()))

    report = {
        "missing_cols": missing_cols,
        "extra_cols": extra_cols,
        "type_errors": type_errors,
        "type_fixes": type_fixes,
    }
    return df, report


def deduplicate(df, logger):
    """
    Elimina duplicados por la columna ID.
    Si no existe la columna ID, se deduplica por todas las columnas.
    """
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


def check_data_quality(df, logger):
    """
    Comprueba la calidad básica de los datos:
    - Conteo de filas
    - Valores nulos por columna
    - Valores únicos de diagnosis
    - Estadísticas descriptivas básicas
    """
    total_rows = df.count()
    logger.info("Total de registros (post-limpieza): %d", total_rows)

    if total_rows == 0:
        logger.warning("El DataFrame está vacío tras la limpieza.")
        return {
            "total_rows": 0,
            "null_counts": {},
            "diagnosis_values": [],
            "null_pct_total": 0.0,
        }

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

    # Porcentaje global de nulls
    total_cells = total_rows * len([c for c in ALL_EXPECTED_COLS if c in df.columns])
    total_nulls = sum(null_counts.values())
    null_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

    # Valores únicos de diagnosis
    diag_values = []
    if EXPECTED_TARGET_COL in df.columns:
        diag_values = [
            row[EXPECTED_TARGET_COL]
            for row in df.select(EXPECTED_TARGET_COL).distinct().collect()
        ]
        logger.info("Valores únicos de '%s': %s", EXPECTED_TARGET_COL, diag_values)

    return {
        "total_rows": total_rows,
        "null_counts": null_counts,
        "diagnosis_values": diag_values,
        "null_pct_total": round(null_pct, 2),
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
    """Escribe un resumen JSON de la ejecución."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"ingesta_clinical_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
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
    logger.info("INICIO - Ingesta del dataset clínico (CSV)")
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

        file_size = os.path.getsize(RAW_CSV_PATH)
        if file_size == 0:
            msg = f"El CSV de entrada está vacío (0 bytes): {RAW_CSV_PATH}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info("CSV de entrada: %s (%d bytes)", RAW_CSV_PATH, file_size)

        # ----- 2. Crear SparkSession -----
        spark = create_spark_session()
        logger.info("SparkSession creada correctamente.")

        # ----- 3. Leer CSV con modo PERMISSIVE -----
        df = read_csv_safe(spark, RAW_CSV_PATH, logger)
        logger.info("Schema inferido:")
        df.printSchema()

        # ----- 4. Manejar filas corruptas -----
        df, n_corrupt = handle_corrupt_records(df, logger)
        execution_log["corrupt_rows"] = n_corrupt

        # ----- 5. Validar schema (schema-evolution friendly) -----
        df, schema_report = validate_schema(df, logger)
        execution_log["schema_validation"] = schema_report

        # ----- 6. Deduplicación -----
        df, n_dupes = deduplicate(df, logger)
        execution_log["duplicates_removed"] = n_dupes

        # ----- 7. Comprobar calidad de datos -----
        quality_report = check_data_quality(df, logger)
        execution_log["data_quality"] = quality_report

        # ----- 8. Seleccionar columnas: esperadas + extra (schema evolution) -----
        # Se conservan TODAS las columnas (esperadas + nuevas) para no perder datos
        available_expected = [c for c in ALL_EXPECTED_COLS if c in df.columns]
        extra_cols = sorted(set(df.columns) - set(ALL_EXPECTED_COLS))
        final_cols = available_expected + extra_cols
        df_clean = df.select(final_cols)
        logger.info(
            "DataFrame final: %d columnas (%d esperadas + %d extra por schema evolution).",
            len(final_cols),
            len(available_expected),
            len(extra_cols),
        )

        # ----- 9. Persistir snapshot Parquet (idempotente) -----
        was_written = persist_parquet(df_clean, RAW_PARQUET_PATH, logger)
        execution_log["parquet_written"] = was_written

        # ----- 10. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["records_ingested"] = quality_report["total_rows"]
        execution_log["errors"] = n_corrupt

        logger.info("=" * 60)
        logger.info("FIN - Ingesta completada con éxito.")
        logger.info(
            "  Registros: %d | Corruptos: %d | Duplicados: %d | Parquet: %s",
            quality_report["total_rows"],
            n_corrupt,
            n_dupes,
            was_written,
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta: %s", e)
        sys.exit(1)

    finally:
        # Escribir log de ejecución
        write_execution_log(execution_log, logger)

        # Cerrar Spark
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
