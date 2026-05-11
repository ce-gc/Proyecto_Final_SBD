"""
03_ingesta_images.py
====================

Uso (dentro del contenedor):
  python jobs/03_ingesta_images.py
  spark-submit jobs/03_ingesta_images.py
"""

import os
import sys
import json
import logging
import hashlib
from datetime import datetime, timezone

try:
    from PIL import Image
    # Desactivar límite de tamaño de imagen para evitar DecompressionBombError
    # en imágenes médicas de alta resolución.
    Image.MAX_IMAGE_PIXELS = None
except ImportError:
    Image = None

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_IMAGES_DIR = os.path.join(DATALAKE_ROOT, "raw", "images")
OUTPUT_META_PATH = os.path.join(DATALAKE_ROOT, "raw", "images_meta")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Extensiones soportadas (ampliadas para cubrir variaciones)
VALID_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp", ".gif",
)

# Categorías conocidas (para logging, pero NO se restringen)
KNOWN_CATEGORIES = {"benign", "malignant", "normal"}

# Forzar re-ejecución si la variable de entorno está activa
FORCE_RERUN = os.environ.get("FORCE_RERUN", "0") == "1"


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
# Procesamiento de imágenes
# ---------------------------------------------------------------------------

def compute_file_hash(filepath, algorithm="md5"):
    """Calcula el hash de un fichero para detectar duplicados binarios."""
    h = hashlib.new(algorithm)
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def extract_image_metadata(image_path, category, logger):
    """
    Extrae metadatos de una imagen individual.
    Try/except por fichero: un error no aborta la ingesta completa.

    Manejo de variaciones:
      - Imágenes de cualquier tamaño (sin límite de píxeles).
      - Cualquier modo de color (RGB, L, RGBA, CMYK, etc.).
      - Ficheros vacíos (0 bytes) detectados previamente.

    Returns:
        dict con metadatos o None si la imagen es corrupta/ilegible.
    """
    try:
        file_size = os.path.getsize(image_path)
        file_name = os.path.basename(image_path)
        extension = os.path.splitext(file_name)[1].lower()

        # Fichero vacío
        if file_size == 0:
            logger.warning("Fichero vacío (0 bytes), se omite: '%s'", image_path)
            return None

        if Image is None:
            raise ImportError("Pillow no está instalado.")

        # Fase 1: Verificar integridad (no abre completamente la imagen)
        try:
            with Image.open(image_path) as img:
                img.verify()
        except Exception as verify_err:
            logger.warning(
                "Imagen no pasa verify() '%s': %s. Intentando lectura directa...",
                image_path,
                verify_err,
            )
            # Algunos formatos (WEBP, TIFF) no soportan verify bien;
            # intentamos lectura directa como fallback

        # Fase 2: Leer metadatos reales (re-abrir tras verify)
        with Image.open(image_path) as img:
            width, height = img.size
            mode = img.mode
            n_channels = len(img.getbands())
            img_format = img.format or extension.lstrip(".")

        file_hash = compute_file_hash(image_path)

        return {
            "name": file_name,
            "path": image_path,
            "category": category,
            "width": width,
            "height": height,
            "channels": n_channels,
            "mode": mode,
            "format": img_format,
            "size_bytes": file_size,
            "file_hash": file_hash,
        }
    except Exception as e:
        logger.warning("Imagen corrupta o ilegible '%s': %s", image_path, e)
        return None


def compute_dimension_stats(all_metadata, logger):
    """
    Calcula estadísticas de dimensiones de las imágenes procesadas.
    Útil para detectar heterogeneidad de tamaños.
    """
    if not all_metadata:
        return {}

    widths = [m["width"] for m in all_metadata]
    heights = [m["height"] for m in all_metadata]

    stats = {
        "width_min": min(widths),
        "width_max": max(widths),
        "width_mean": round(sum(widths) / len(widths), 1),
        "height_min": min(heights),
        "height_max": max(heights),
        "height_mean": round(sum(heights) / len(heights), 1),
        "unique_sizes": len(set(zip(widths, heights))),
        "modes": list(set(m["mode"] for m in all_metadata)),
        "formats": list(set(m["format"] for m in all_metadata)),
        "channels": list(set(m["channels"] for m in all_metadata)),
    }

    logger.info("Estadísticas de dimensiones:")
    logger.info(
        "  Width:  min=%d, max=%d, media=%.1f",
        stats["width_min"], stats["width_max"], stats["width_mean"],
    )
    logger.info(
        "  Height: min=%d, max=%d, media=%.1f",
        stats["height_min"], stats["height_max"], stats["height_mean"],
    )
    logger.info("  Tamaños únicos: %d", stats["unique_sizes"])
    logger.info("  Modos de color: %s", stats["modes"])
    logger.info("  Formatos: %s", stats["formats"])
    logger.info("  Canales: %s", stats["channels"])

    return stats


def detect_duplicate_images(all_metadata, logger):
    """
    Detecta imágenes duplicadas por hash de contenido.
    No las elimina, solo las registra.
    """
    hash_map = {}
    duplicates = []
    for m in all_metadata:
        h = m.get("file_hash")
        if h is None:
            continue
        if h in hash_map:
            duplicates.append((m["path"], hash_map[h]))
        else:
            hash_map[h] = m["path"]

    if duplicates:
        logger.warning(
            "%d imágenes duplicadas detectadas (por hash):", len(duplicates),
        )
        for dup, orig in duplicates[:10]:
            logger.warning("  → '%s' es duplicado de '%s'", dup, orig)
    else:
        logger.info("No se detectaron imágenes duplicadas.")

    return duplicates


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("INICIO - Ingesta de metadatos de imágenes")
    logger.info("=" * 60)
    logger.info("DATALAKE_ROOT: %s", DATALAKE_ROOT)

    if Image is None:
        logger.error("La librería Pillow (PIL) no está instalada. Abortando.")
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
        # ----- 1. Verificar que el directorio de imágenes existe -----
        if not os.path.exists(RAW_IMAGES_DIR):
            msg = f"No se encuentra el directorio de imágenes: {RAW_IMAGES_DIR}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        # ----- 2. Idempotencia -----
        if os.path.exists(OUTPUT_META_PATH) and not FORCE_RERUN:
            logger.info(
                "Los metadatos ya existen en '%s'. Se omite (idempotencia). "
                "Para forzar, usa FORCE_RERUN=1.",
                OUTPUT_META_PATH,
            )
            execution_log["status"] = "SKIPPED"
            execution_log["reason"] = "Output ya existe (idempotencia)"
            return

        # ----- 3. Recorrer árbol de directorios (categorías dinámicas) -----
        all_metadata = []
        corrupt_count = 0
        empty_count = 0
        skipped_extensions = []
        category_counts = {}
        discovered_categories = set()

        for root, dirs, files in os.walk(RAW_IMAGES_DIR):
            # Inferir categoría del nombre del directorio
            category = os.path.basename(root)

            # Omitir el directorio raíz (RAW_IMAGES_DIR) en sí
            if os.path.abspath(root) == os.path.abspath(RAW_IMAGES_DIR):
                # Procesar archivos sueltos en la raíz con categoría "uncategorized"
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    if ext in VALID_EXTENSIONS:
                        full_path = os.path.join(root, f)
                        meta = extract_image_metadata(full_path, "uncategorized", logger)
                        if meta:
                            all_metadata.append(meta)
                            category_counts["uncategorized"] = \
                                category_counts.get("uncategorized", 0) + 1
                        elif os.path.getsize(full_path) == 0:
                            empty_count += 1
                        else:
                            corrupt_count += 1
                    else:
                        skipped_extensions.append(f)
                continue

            # Cualquier subdirectorio es una categoría válida
            discovered_categories.add(category)

            # Loguear categorías nuevas no conocidas previamente
            if category not in KNOWN_CATEGORIES:
                logger.info(
                    "Categoría nueva descubierta (schema evolution): '%s'",
                    category,
                )

            for f in files:
                full_path = os.path.join(root, f)
                ext = os.path.splitext(f)[1].lower()

                # Extensión no reconocida
                if ext not in VALID_EXTENSIONS:
                    skipped_extensions.append(f)
                    logger.debug("Extensión no reconocida (ignorada): %s", f)
                    continue

                # Extraer metadatos (try/except por fichero)
                meta = extract_image_metadata(full_path, category, logger)
                if meta:
                    all_metadata.append(meta)
                    category_counts[category] = category_counts.get(category, 0) + 1
                else:
                    if os.path.getsize(full_path) == 0:
                        empty_count += 1
                    else:
                        corrupt_count += 1

        logger.info(
            "Procesadas %d imágenes válidas. Corruptas: %d. Vacías: %d. "
            "Extensiones ignoradas: %d.",
            len(all_metadata), corrupt_count, empty_count, len(skipped_extensions),
        )
        logger.info("Categorías descubiertas: %s", sorted(discovered_categories))
        for cat, count in sorted(category_counts.items()):
            logger.info("  Categoría '%s': %d imágenes", cat, count)

        # ----- 4. Estadísticas de dimensiones -----
        dim_stats = compute_dimension_stats(all_metadata, logger)
        execution_log["dimension_stats"] = dim_stats

        # ----- 5. Detección de duplicados -----
        duplicates = detect_duplicate_images(all_metadata, logger)
        execution_log["duplicate_images"] = len(duplicates)

        if not all_metadata:
            logger.warning("No se encontraron imágenes válidas. No se generará Parquet.")
            execution_log["status"] = "SUCCESS"
            execution_log["images_processed"] = 0
            execution_log["corrupt_files"] = corrupt_count
            execution_log["empty_files"] = empty_count
            return

        # ----- 6. Crear SparkSession y persistir Parquet -----
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
            StructField("channels", IntegerType(), True),
            StructField("mode", StringType(), True),
            StructField("format", StringType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("file_hash", StringType(), True),
        ])

        df_meta = spark.createDataFrame(all_metadata, schema=schema)

        logger.info("Escribiendo metadatos Parquet en '%s'...", OUTPUT_META_PATH)
        df_meta.coalesce(1).write.mode("overwrite").parquet(OUTPUT_META_PATH)
        logger.info("Metadatos Parquet escritos correctamente.")

        # ----- 7. Resumen final -----
        execution_log["status"] = "SUCCESS"
        execution_log["images_processed"] = len(all_metadata)
        execution_log["corrupt_files"] = corrupt_count
        execution_log["empty_files"] = empty_count
        execution_log["skipped_extensions"] = len(skipped_extensions)
        execution_log["category_counts"] = category_counts
        execution_log["discovered_categories"] = sorted(discovered_categories)
        execution_log["errors"] = corrupt_count + empty_count

        logger.info("=" * 60)
        logger.info("FIN - Ingesta de metadatos completada.")
        logger.info(
            "  Imágenes: %d | Corruptas: %d | Vacías: %d | "
            "Extensiones ignoradas: %d | Categorías: %d",
            len(all_metadata),
            corrupt_count,
            empty_count,
            len(skipped_extensions),
            len(discovered_categories),
        )
        logger.info("=" * 60)

    except Exception as e:
        execution_log["status"] = "FAILED"
        execution_log["error_message"] = str(e)
        logger.exception("Error durante la ingesta de imágenes: %s", e)
        sys.exit(1)

    finally:
        write_execution_log(execution_log, logger)
        if spark:
            spark.stop()
            logger.info("SparkSession cerrada.")


def write_execution_log(log_data, logger):
    """Escribe un resumen JSON de la ejecución."""
    os.makedirs(LOG_DIR, exist_ok=True)
    summary_file = os.path.join(
        LOG_DIR,
        f"ingesta_images_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Resumen de ejecución guardado en '%s'.", summary_file)


if __name__ == "__main__":
    main()
