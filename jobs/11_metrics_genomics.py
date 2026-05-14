"""
11_metrics_genomics.py
======================
Calcula métricas y realiza modelos sobre el dataset genómico (K-Means e Isolation Forest).
Genera análisis geográfico, clustering y detección de anomalías.
"""

import os
import json
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Configuración de rutas
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
RESULTS_DIR = os.path.join(DATALAKE_ROOT, "Results")
os.makedirs(RESULTS_DIR, exist_ok=True)

def main():
    print("Iniciando cálculo de métricas genómicas...")
    
    # Cargar con pandas (pyarrow debe estar instalado)
    df = pd.read_parquet(CLEANSE_GENOMICS_PATH)
    
    metrics_report = {}
    
    # Sincronizar selección de columnas con 08_curated_clustering.py
    # En pandas, seleccionamos columnas numéricas que no sean las de metadata excluidas en el job 08
    excluded_cols = ["age_at_diagnosis", "survival_months", "recurrence_free_months", "patient_id", "id"]
    expression_cols = [c for c in df.select_dtypes(include=[np.number]).columns 
                       if c not in excluded_cols]
    
    # --- Distribución por país ---
    geo_metrics = {}
    if 'country' in df.columns:
        country_counts = df['country'].value_counts().to_dict()
        geo_metrics["counts"] = {str(k): int(v) for k, v in country_counts.items()}
        
        # Agrupar variables numéricas por país
        geo_stats = df.groupby('country')[expression_cols[:10]].agg(['mean', 'std'])
        stats_dict = {}
        for country in geo_stats.index:
            stats_dict[country] = {}
            for col in expression_cols[:5]: 
                stats_dict[country][col] = {
                    "mean": float(geo_stats.loc[country, (col, 'mean')]),
                    "std": float(geo_stats.loc[country, (col, 'std')])
                }
        geo_metrics["stats"] = stats_dict
    
    metrics_report["geographic_distribution"] = geo_metrics
    metrics_report["geographic_interpretation"] = "Analizamos la representación geográfica para detectar sesgos. Las diferencias en las medias de expresión génica o en el recuento por país muestran cómo los datos sintéticos capturan la diversidad regional, lo cual es crítico para que los modelos no se sesguen hacia poblaciones mayoritarias."

    # --- Clustering K-Means ---
    # Escalar datos numéricos para K-Means
    X = df[expression_cols].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    inertias = []
    silhouette_scores = {}
    
    for k in range(2, 7):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        inertias.append({"k": k, "inertia": float(kmeans.inertia_)})
        if k > 1:
            # Usar muestra para 100k registros para evitar O(N^2)
            score = float(silhouette_score(X_scaled, labels, sample_size=1000, random_state=42))
            silhouette_scores[k] = score

    # Seleccionamos k=3 como en el job 08
    chosen_k = 3
    kmeans_opt = KMeans(n_clusters=chosen_k, random_state=42, n_init=10)
    df['cluster'] = kmeans_opt.fit_predict(X_scaled)
    
    sil_score = float(silhouette_score(X_scaled, df['cluster'], sample_size=1000, random_state=42))
    # Para sample_sil_values, solo podemos calcularlo para una muestra si queremos que termine rápido
    sample_indices = np.random.RandomState(42).choice(len(df), size=min(5000, len(df)), replace=False)
    sample_sil_values = silhouette_samples(X_scaled[sample_indices], df.loc[sample_indices, 'cluster'])
    
    cluster_metrics = []
    for i in range(chosen_k):
        cluster_data = df[df['cluster'] == i]
        # Calcular silhouette medio del cluster usando la muestra
        cluster_mask = df.loc[sample_indices, 'cluster'] == i
        if cluster_mask.any():
            mean_sil_cluster = float(sample_sil_values[cluster_mask].mean())
        else:
            mean_sil_cluster = 0.0
            
        cluster_metrics.append({
            "cluster_id": i,
            "size": int(len(cluster_data)),
            "mean_silhouette": mean_sil_cluster,
            "profile_mean": {col: float(cluster_data[col].mean()) for col in expression_cols[:5]}
        })

    metrics_report["kmeans_clustering"] = {
        "inertias": inertias,
        "silhouette_scores": silhouette_scores,
        "chosen_k": chosen_k,
        "global_silhouette": sil_score,
        "cluster_details": cluster_metrics
    }
    metrics_report["kmeans_interpretation"] = f"El K-Means (K={chosen_k}) obtiene un silhouette score global de {sil_score:.3f}. La inercia y los perfiles de los clusters revelan distintos subtipos biológicos en los pacientes; esto sugiere que el cáncer se presenta con firmas genómicas diferenciadas (agrupadas aquí en {chosen_k} grupos), lo que apoya tratamientos personalizados."

    # --- Isolation Forest (Detección de anomalías) ---
    iso = IsolationForest(contamination=0.01, random_state=42)
    df['anomaly_label'] = iso.fit_predict(X_scaled)  # -1 for outliers, 1 for inliers
    
    anomalies_count = int((df['anomaly_label'] == -1).sum())
    total_count = len(df)
    
    # Distribución de anomalías por país y por cluster
    if 'country' in df.columns:
        anom_by_country = df[df['anomaly_label'] == -1]['country'].value_counts().to_dict()
    else:
        anom_by_country = {}
        
    anom_by_cluster = df[df['anomaly_label'] == -1]['cluster'].value_counts().to_dict()

    metrics_report["isolation_forest"] = {
        "anomalies_detected": anomalies_count,
        "anomalies_percentage": float(anomalies_count / total_count * 100),
        "anomalies_by_country": {str(k): int(v) for k, v in anom_by_country.items()},
        "anomalies_by_cluster": {str(k): int(v) for k, v in anom_by_cluster.items()}
    }
    metrics_report["isolation_forest_interpretation"] = f"Isolation Forest marcó un {float(anomalies_count / total_count * 100):.2f}% de los registros como anómalos. Estos pacientes presentan perfiles de expresión génica extremadamente raros, lo cual puede deberse a casos de cáncer muy atípicos, mutaciones severas o ruido de medición. Detectarlos es vital para no degradar los modelos predictivos."

    # Guardar métricas
    output_path = os.path.join(RESULTS_DIR, "metrics_genomics.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics_report, f, indent=4, ensure_ascii=False)
        
    print(f"Métricas genómicas guardadas en {output_path}")

if __name__ == "__main__":
    main()
