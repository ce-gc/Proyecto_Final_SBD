"""
08_curated_clustering.py
========================
Aplica PCA y K-Means sobre los datos genómicos para identificar perfiles de pacientes.

Entrada: /datalake/cleanse/genomics/
Salida:  /datalake/curated/genomics_clustering/
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F

# Configuración de rutas
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
CURATED_CLUSTERING_PATH = os.path.join(DATALAKE_ROOT, "curated", "genomics_clustering")

def main():
    spark = SparkSession.builder \
        .appName("Curated - Clustering Genomico") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # 1. Cargar datos genómicos (ya limpios en cleanse)
    df = spark.read.parquet(CLEANSE_GENOMICS_PATH)
    
    # Seleccionar automáticamente solo columnas numéricas para el análisis
    expression_cols = [c for c, t in df.dtypes if t in ("double", "float", "int", "bigint") 
                       and c not in ["age_at_diagnosis", "survival_months", "recurrence_free_months"]]
    
    metadata_cols = [c for c in df.columns if c not in expression_cols]

    # 2. Vectorizar y Escalar
    assembler = VectorAssembler(inputCols=expression_cols, outputCol="features", handleInvalid="skip")
    df_vector = assembler.transform(df)
    
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vector)
    df_scaled = scaler_model.transform(df_vector)

    # 3. PCA (Reducir a 2 componentes para visualización)
    pca = PCA(k=2, inputCol="scaled_features", outputCol="pca_features")
    pca_model = pca.fit(df_scaled)
    df_pca = pca_model.transform(df_scaled)

    # 4. K-Means
    kmeans = KMeans(featuresCol="pca_features", predictionCol="cluster_id", k=3, seed=42)
    kmeans_model = kmeans.fit(df_pca)
    df_final = kmeans_model.transform(df_pca)

    # 5. Guardar una muestra relevante para el análisis
    # Solo columnas de metadatos, los 2 componentes de PCA y el cluster_id
    cols_to_keep = metadata_cols + ["pca_features", "cluster_id"]
    df_final.select(cols_to_keep) \
        .write.mode("overwrite").parquet(CURATED_CLUSTERING_PATH)

    print(f"Job 08 finalizado. Datos guardados en {CURATED_CLUSTERING_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
