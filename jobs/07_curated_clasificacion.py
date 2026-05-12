"""
07_curated_clasificacion.py
===========================
Prepara los datos clínicos de la capa cleanse para el análisis de clasificación.
Extrae la importancia de las variables para el diagnóstico.

Entrada: /datalake/cleanse/clinical/
Salida:  /datalake/curated/clinical_classification/
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

# Configuración
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_CLINICAL_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
CURATED_BASE_PATH = os.path.join(DATALAKE_ROOT, "curated", "clinical_classification")
OUTPUT_DATA_PATH = os.path.join(CURATED_BASE_PATH, "data")
OUTPUT_META_PATH = os.path.join(CURATED_BASE_PATH, "metadata")

NUMERIC_COLS = [
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

def main():
    spark = SparkSession.builder \
        .appName("Curated - Clasificacion Clinica") \
        .master("local[*]") \
        .getOrCreate()

    # 1. Cargar datos de cleanse
    df = spark.read.parquet(CLEANSE_CLINICAL_PATH)
    
    # 2. Vectorizar y Escalar
    assembler = VectorAssembler(inputCols=NUMERIC_COLS, outputCol="features")
    df_vector = assembler.transform(df)
    
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(df_vector)
    df_curated = scaler_model.transform(df_vector)

    # 3. Clasificación ligera para Feature Importance (100 árboles)
    # Convertimos diagnosis a label numérico: M=1, B=0
    df_curated = df_curated.withColumn("label", F.when(F.col("diagnosis") == "M", 1.0).otherwise(0.0))
    
    rf = RandomForestClassifier(labelCol="label", featuresCol="scaled_features", numTrees=100, seed=42)
    rf_model = rf.fit(df_curated)
    
    # Extraer importancia
    importances = rf_model.featureImportances.toArray()
    feature_importance_list = sorted(
        [{"feature": f, "importance": float(i)} for f, i in zip(NUMERIC_COLS, importances)],
        key=lambda x: x["importance"], reverse=True
    )

    # 4. Guardar Metadatos (Top 5 features)
    os.makedirs(OUTPUT_META_PATH, exist_ok=True)
    with open(os.path.join(OUTPUT_META_PATH, "feature_importance.json"), "w") as f:
        json.dump(feature_importance_list[:5], f, indent=4)

    # 5. Guardar Datos Curated (solo columnas necesarias para análisis futuro)
    df_curated.select("id", "diagnosis", "scaled_features", "label") \
        .write.mode("overwrite").parquet(OUTPUT_DATA_PATH)

    print(f"Job 07 finalizado. Top 5 features: {feature_importance_list[:5]}")
    spark.stop()

if __name__ == "__main__":
    main()
