"""
09_curated_anomalias.py
=======================
Detección de anomalías en datos genómicos utilizando un enfoque de puntuación de distancia (Z-score)
sobre componentes principales (PCA), que sirve como preparación para Isolation Forest.
Guarda solo el 1% de los registros más inusuales.

Entrada: /datalake/cleanse/genomics/
Salida:  /datalake/curated/genomics_anomalies/
"""

import os
import sys
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuración
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
CURATED_ANOMALIES_PATH = os.path.join(DATALAKE_ROOT, "curated", "genomics_anomalies")

def main():
    spark = SparkSession.builder \
        .appName("Curated - Deteccion Anomalias") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # 1. Cargar datos
    df = spark.read.parquet(CLEANSE_GENOMICS_PATH)
    
    # Seleccionar automáticamente solo columnas numéricas para el análisis
    expression_cols = [c for c, t in df.dtypes if t in ("double", "float", "int", "bigint") 
                       and c not in ["age_at_diagnosis", "survival_months", "recurrence_free_months"]]
    
    metadata_cols = [c for c in df.columns if c not in expression_cols]

    # 2. Preparar Features (PCA para reducir ruido y encontrar dimensiones críticas)
    assembler = VectorAssembler(inputCols=expression_cols, outputCol="features", handleInvalid="skip")
    df_vector = assembler.transform(df)
    
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    df_scaled = scaler.fit(df_vector).transform(df_vector)
    
    pca = PCA(k=5, inputCol="scaled_features", outputCol="pca_features")
    df_pca = pca.fit(df_scaled).transform(df_scaled)

    # 3. Calcular Anomaly Score (Distancia Euclidea al origen en espacio PCA)
    # Las anomalías suelen estar lejos del centro de la masa de datos
    def vector_to_norm(v):
        import numpy as np
        return float(np.linalg.norm(v.toArray()))

    norm_udf = F.udf(vector_to_norm, "double")
    df_scored = df_pca.withColumn("anomaly_score", norm_udf(F.col("pca_features")))

    # 4. Filtrar el top 1% de anomalías (Contamination = 0.01)
    threshold = df_scored.stat.approxQuantile("anomaly_score", [0.99], 0.001)[0]
    
    df_anomalies = df_scored.filter(F.col("anomaly_score") >= threshold)

    # 5. Guardar resultados
    # Guardamos los metadatos y el score para el Heatmap solicitado
    df_anomalies.select(metadata_cols + ["anomaly_score"]) \
        .write.mode("overwrite").parquet(CURATED_ANOMALIES_PATH)

    print(f"Job 09 finalizado. {df_anomalies.count()} anomalías detectadas y guardadas.")
    spark.stop()

if __name__ == "__main__":
    main()
