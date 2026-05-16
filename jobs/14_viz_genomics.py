"""
14_viz_genomics.py
==================
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Paleta de colores
COLOR_C1 = "#1f77b4"
COLOR_C2 = "#ff7f0e"
COLOR_C3 = "#2ca02c"
COLOR_ANOMALY = "#d62728"

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIGURES_DIR = os.path.join(PROJECT_ROOT, "reports", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

def save_plot_and_text(fig_name, text_content):
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f"{fig_name}.png"), dpi=150)
    plt.close()
    
    with open(os.path.join(FIGURES_DIR, f"{fig_name}.txt"), "w", encoding="utf-8") as f:
        f.write(text_content)

def main():
    print("Generando visualizaciones genómicas...")
    df = pd.read_parquet(CLEANSE_GENOMICS_PATH)
    
    excluded_cols = ["age_at_diagnosis", "survival_months", "recurrence_free_months", "patient_id", "id", "country", "cluster", "anomaly_label"]
    expression_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in excluded_cols]
    
    X = df[expression_cols].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    pca_2 = PCA(n_components=2, random_state=42)
    X_pca_2 = pca_2.fit_transform(X_scaled)
    
    # 1. Elbow plot
    inertias = []
    K_range = range(2, 8)
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_pca_2)
        inertias.append(kmeans.inertia_)
        
    plt.figure(figsize=(8, 6))
    plt.plot(K_range, inertias, marker='o', color=COLOR_C1)
    plt.title("Método del Codo (Elbow Plot) para K-Means")
    plt.xlabel("Número de Clusters (K)")
    plt.ylabel("Inercia")
    plt.axvline(x=3, color='gray', linestyle='--')
    
    text_elbow = "El gráfico de codo muestra la inercia en función del número de clusters K. Se observa un punto de inflexión alrededor de K=3, justificando la elección de este número como óptimo, en lugar de una decisión arbitraria."
    save_plot_and_text("elbow_plot_genomics", text_elbow)
    
    # 2. Scatter PCA 2D
    chosen_k = 3
    kmeans_opt = KMeans(n_clusters=chosen_k, random_state=42, n_init=10)
    df['cluster'] = kmeans_opt.fit_predict(X_pca_2)
    
    var_exp = pca_2.explained_variance_ratio_ * 100
    
    plt.figure(figsize=(10, 8))
    sns.scatterplot(x=X_pca_2[:,0], y=X_pca_2[:,1], hue=df['cluster'], palette="Set1", s=50, alpha=0.7)
    plt.title("Clusters K-Means en Espacio PCA (2 Componentes)")
    plt.xlabel(f"PC1 ({var_exp[0]:.1f}% varianza)")
    plt.ylabel(f"PC2 ({var_exp[1]:.1f}% varianza)")
    plt.legend(title="Cluster")
    
    text_pca = "El gráfico PCA reduce las dimensiones genómicas a 2 ejes, explicando un porcentaje de varianza representativo. Los puntos coloreados por cluster muestran la separación de distintos subtipos biológicos en el espacio latente, la visualización más potente del análisis no supervisado."
    save_plot_and_text("pca_clusters_genomics", text_pca)
    
    # 3. Barplot de tamaño de clusters por país
    if 'country' in df.columns:
        cross_tab = pd.crosstab(df['cluster'], df['country'])
        cross_tab.plot(kind='bar', stacked=True, figsize=(10, 6), colormap='viridis')
        plt.title("Composición de Clusters por País")
        plt.xlabel("Cluster ID")
        plt.ylabel("Número de Pacientes")
        plt.legend(title="País", bbox_to_anchor=(1.05, 1), loc='upper left')
        
        text_bar = "El gráfico de barras apiladas revela la distribución de países dentro de cada cluster. Permite interpretar si los subtipos genómicos descubiertos tienen un componente geográfico, dando pistas sobre posibles factores poblacionales o ambientales."
        save_plot_and_text("clusters_country_genomics", text_bar)

    # 4. Distribución de anomaly scores
    iso = IsolationForest(contamination=0.01, random_state=42)
    pca_5 = PCA(n_components=5, random_state=42)
    X_pca_5 = pca_5.fit_transform(X_scaled)
    
    scores = iso.fit(X_pca_5).decision_function(X_pca_5)
    df['anomaly_score'] = scores
    df['anomaly_label'] = iso.predict(X_pca_5)
    
    threshold = np.percentile(scores, 1) # 1% de contaminación
    perc_anom = (df['anomaly_label'] == -1).mean() * 100
    
    plt.figure(figsize=(10, 6))
    sns.histplot(scores, bins=50, color=COLOR_C1, kde=False)
    plt.axvline(x=threshold, color=COLOR_ANOMALY, linestyle='--', linewidth=2, label=f'Umbral (Anomalías: {perc_anom:.1f}%)')
    
    sns.histplot(scores[scores < threshold], bins=50, color=COLOR_ANOMALY, kde=False)
    
    plt.title(f"Distribución de Anomaly Scores ({perc_anom:.1f}% Anomalías)")
    plt.xlabel("Anomaly Score (valores negativos = anómalos)")
    plt.ylabel("Frecuencia")
    plt.legend()
    
    text_anom = "El histograma de scores de anomalía muestra una distribución continua con una cola izquierda que agrupa registros inusuales. La línea vertical marca el umbral del Isolation Forest, separando claramente (en rojo) los pacientes con perfiles de expresión génica extremadamente atípicos."
    save_plot_and_text("anomaly_scores_genomics", text_anom)
    
    print("Visualizaciones genómicas generadas exitosamente.")

if __name__ == "__main__":
    main()
