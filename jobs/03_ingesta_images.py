"""
03_ingesta_images.py
====================
Job de ingesta para el dataset de imagenes (no estructurado).

Estrategia:
  - Recorrido del arbol de directorios con os.walk para localizar PNG/JPG.
  - Extraccion de metadatos por imagen: nombre, ruta, clase, ancho, alto, modo, bytes.
  - Creacion de DataFrame PySpark y persistencia en raw/images_meta/ como Parquet.
  - Las imagenes originales se mantienen en Raw sin modificar.
  - Deteccion y logging de imagenes corruptas/extensiones no reconocidas.
  - Idempotente: verifica existencia antes de escribir.
  - Try/except por fichero: un error no aborta la ingesta completa.

Uso (dentro del contenedor):
  python jobs/03_ingesta_images.py
  spark-submit jobs/03_ingesta_images.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

try:
    from PIL import Image
except ImportError:
    Image = None

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_IMAGES_DIR = os.path.join(DATALAKE_ROOT, "raw", "images")
OUTPUT_META_PATH = os.path.join(DATALAKE_ROOT, "raw", "images_meta")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

VALID_EXTENSIONS = (".png", ".jpg", ".jpeg")
EXPECTED_CATEGORIES = {"benign", "malignant", "normal"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configura logging a consola y a archivo."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"ingesta_images_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("ingesta_images")
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
# Procesamiento de imagenes
# ---------------------------------------------------------------------------

def extract_image_metadata(image_path, category, logger):
    """
    Extrae metadatos de una imagen individual.
    Try/except por fichero: un error no aborta la ingesta completa.
    
    Returns:
        dict con metadatos o None si la imagen es corrupta/ilegible.
    """
    try:
        file_size = os.path.getsize(image_path)
        file_name = os.path.basename(image_path)

        if Image is None:
            raise ImportError("Pillow no esta instalado.")

        with Image.open(image_path) as img:
            img.verify()  # Verificar que la imagen no esta corrupta

        # Re-abrir despues de verify() (verify cierra el archivo)
        with Image.open(image_path) as img:
            width, height = img.size
            mode = img.mode

        return {
            "name": file_name,
            "path": image_path,
            "category": category,
            "width": width,
            "height": height,
            "mode": mode,
            "size_bytes": file_size,
        }
    except Exception as e:
        logger.warning("Imagen corrupta o ilegible '%s': %s", image_path, e)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Ingesta de metadatos de imagenes")
    logger.info("=" * 60)
    logger.info("DATALAKE_ROOT: %s", DATALAKE_ROOT)

    if Image is None:
        logger.error("La libreria Pillow (PIL) no esta instalada. Abortando.")
        sys.exit(1)

    execution_log = {
        "job": "03_ingesta_images",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "STARTED",
        "input_dir": RAW_IMAGES_DIR,
        "output_path": OUTPUT_META_PATH,
    }

    spark = None
    try:
        # ----- 1. Verificar que el directorio de imagenes existe -----
        if not os.path.exists(RAW_IMAGES_DIR):
            msg = f"No se encuentra el directorio de imagenes: {RAW_IMAGES_DIR}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        # ----- 2. Idempotencia: verificar si ya se ha ejecutado -----
        if os.path.exists(OUTPUT_META_PATH):
            logger.info(
                "Los metadatos ya existen en '%s'. Se omite (idempotencia).",
                OUTPUT_META_PATH,
            )
            execution_log["status"] = "SKIPPED"
            execution_log["reason"] = "Output ya existe (idempotencia)"
            return

        # ----- 3. Recorrer arbol de directorios -----
        all_metadata = []
        corrupt_count = 0
        skipped_extensions = []
        category_counts = {}

        for root, dirs, files in os.walk(RAW_IMAGES_DIR):
            # Inferir categoria del nombre del directorio
            category = os.path.basename(root)

            # Solo procesar directorios de categoria conocidos
            if category not in EXPECTED_CATEGORIES:
                # Si hay archivos en directorios no esperados, loguear
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    if ext in VALID_EXTENSIONS:
                        logger.debug(
                            "Archivo en directorio no esperado '%s': %s",
                            category,
                            f,
                        )
                continue

            for f in files:
                full_path = os.path.join(root, f)
                ext = os.path.splitext(f)[1].lower()

                # Extension no reconocida
                if ext not in VALID_EXTENSIONS:
                    skipped_extensions.append(f)
                    logger.debug("Extension no reconocida (ignorada): %s", f)
                    continue

                # Extraer metadatos (try/except por fichero)
                meta = extract_image_metadata(full_path, category, logger)
                if meta:
                    all_metadata.append(meta)
                    category_counts[category] = category_counts.get(category, 0) + 1
                else:
                    corrupt_count += 1

        logger.info(
            "Procesadas %d imagenes. Corruptas: %d. Extensiones ignoradas: %d.",
            len(all_metadata),
            corrupt_count,
            len(skipped_extensions),
        )
        for cat, count in sorted(category_counts.items()):
            logger.info("  Categoria '%s': %d imagenes", cat, count)

        if not all_metadata:
            logger.warning("No se encontraron imagenes validas. No se generara Parquet.")
            execution_log["status"] = "SUCCESS"
            execution_log["images_processed"] = 0
            execution_log["corrupt_files"] = corrupt_count
            return

        # ----- 4. Crear SparkSession y persistir Parquet -----
        spark = (
            SparkSession.builder
            .appName("Ingesta Image Metadata")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )

        schema = StructType([
            StructField("name", StringType(), True),
            StructField("path", StringType(), True),
            StructField("category", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
            StructField("mode", StringType(), True),
            StructField("size_bytes", LongType(), True),
        ])

        df_meta = spark.createDataFrame(all_metadata, schema=schema)

        logger.info("Escribiendo metadatos Parquet en '%s'...", OUTPUT_META_PATH)
        df_meta.coalesce(1).write.mode("overwrite").parquet(OUTPUT_META_PATH)
        logger.info("Metadatos Parquet escritos correctamente.")

        # ----- 5. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["images_processed"] = len(all_metadata)
        execution_log["corrupt_files"] = corrupt_count
        execution_log["skipped_extensions"] = len(skipped_extensions)
        execution_log["category_counts"] = category_counts
        execution_log["errors"] = corrupt_count

        logger.info("=" * 60)
        logger.info("FIN - Ingesta de metadatos completada.")
        logger.info(
            "  Imagenes: %d | Corruptas: %d | Extensiones ignoradas: %d",
            len(all_metadata),
            corrupt_count,
            len(skipped_extensions),
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta de imagenes: %s", e)
        sys.exit(1)

    finally:
        write_execution_log(execution_log, logger)
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecucion."""
    os.makedirs(LOG_DIR, exist_ok=True)
    summary_file = os.path.join(
        LOG_DIR,
        f"ingesta_images_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecucion guardado en '%s'.", summary_file)


if __name__ == "__main__":
    main()
