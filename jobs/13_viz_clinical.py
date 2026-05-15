"""
13_viz_clinical.py
==================
Genera visualizaciones para el dataset clínico (correlaciones, separabilidad, 
importancia de variables, curva ROC y matriz de confusión).
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc, confusion_matrix
from sklearn.preprocessing import StandardScaler

# Paleta de colores consistente
COLOR_M = "#d62728"  # Rojo para Maligno
COLOR_B = "#1f77b4"  # Azul para Benigno
COLOR_OTHER = "#2ca02c" # Verde

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_CLINICAL_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
FIGURES_DIR = os.path.join(DATALAKE_ROOT, "curated", "reports", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

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

def save_plot_and_text(fig_name, text_content):
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f"{fig_name}.png"), dpi=150)
    plt.close()
    
    with open(os.path.join(FIGURES_DIR, f"{fig_name}.txt"), "w", encoding="utf-8") as f:
        f.write(text_content)

def main():
    print("Generando visualizaciones clínicas...")
    df = pd.read_parquet(CLEANSE_CLINICAL_PATH)
    
    # 1. Matriz de correlación
    plt.figure(figsize=(12, 10))
    corr_matrix = df[NUMERIC_COLS].corr()
    sns.heatmap(corr_matrix, cmap="RdBu_r", center=0, annot=False, vmin=-1, vmax=1)
    plt.title("Matriz de Correlación de Variables Clínicas")
    
    text_corr = "Se observa alta correlación positiva (zonas rojas intensas) entre variables de tamaño como radio, área y perímetro, lo que indica redundancia. Estas variables podrían descartarse o combinarse en futuros modelos para evitar multicolinealidad."
    save_plot_and_text("corr_matrix_clinical", text_corr)

    # 2. Boxplots para variables con mayor ratio de Fisher
    fisher_ratios = {}
    for col in NUMERIC_COLS:
        mean_M = df[df['diagnosis'] == 'M'][col].mean()
        var_M = df[df['diagnosis'] == 'M'][col].var()
        mean_B = df[df['diagnosis'] == 'B'][col].mean()
        var_B = df[df['diagnosis'] == 'B'][col].var()
        fisher_ratios[col] = ((mean_M - mean_B) ** 2) / (var_M + var_B + 1e-9)
        
    top_fisher = sorted(fisher_ratios, key=fisher_ratios.get, reverse=True)[:6]
    
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    for i, col in enumerate(top_fisher):
        ax = axes[i//3, i%3]
        sns.boxplot(x='diagnosis', y=col, hue='diagnosis', data=df, palette={"M": COLOR_M, "B": COLOR_B}, ax=ax, legend=False)
        ax.set_title(f"Distribución de {col}")
    
    text_box = "Los boxplots muestran las 6 variables con mayor ratio de Fisher. Visualmente es evidente la gran separabilidad de clases: los valores de los casos malignos (M) son sistemáticamente superiores a los benignos (B), lo que justifica su excelente poder predictivo."
    save_plot_and_text("boxplots_fisher_clinical", text_box)
    
    # Modelo Random Forest
    X = df[NUMERIC_COLS]
    y = df['diagnosis'].apply(lambda x: 1 if x == 'M' else 0)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train_scaled, y_train)
    y_pred = rf.predict(X_test_scaled)
    y_prob = rf.predict_proba(X_test_scaled)[:, 1]
    
    # 3. Feature Importance
    importances = rf.feature_importances_
    indices = np.argsort(importances)[::-1][:15] # Top 15
    
    plt.figure(figsize=(10, 8))
    sns.barplot(x=importances[indices], y=np.array(NUMERIC_COLS)[indices], color=COLOR_B)
    plt.title("Top 15 Importancia de Variables (Random Forest)")
    plt.xlabel("Importancia Relativa")
    
    text_feat = "El gráfico destaca las 15 variables más importantes según el Random Forest. Las variables relacionadas con el tamaño y la concavidad dominan la decisión del modelo, indicando qué aspectos morfométricos importan clínicamente."
    save_plot_and_text("feature_importance_clinical", text_feat)

    # 4. Curva ROC
    fpr, tpr, _ = roc_curve(y_test, y_prob)
    roc_auc = auc(fpr, tpr)
    
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, color=COLOR_M, lw=2, label=f'ROC curve (AUC = {roc_auc:.3f})')
    plt.plot([0, 1], [0, 1], color='gray', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('Tasa de Falsos Positivos')
    plt.ylabel('Tasa de Verdaderos Positivos')
    plt.title('Curva ROC - Clasificador Clínico')
    plt.legend(loc="lower right")
    
    text_roc = f"La curva ROC muestra el rendimiento del modelo, logrando un AUC de {roc_auc:.3f}. La curva se acerca a la esquina superior izquierda, muy por encima del clasificador aleatorio, confirmando la alta fiabilidad en la clasificación."
    save_plot_and_text("roc_curve_clinical", text_roc)

    # 5. Matriz de confusión
    cm = confusion_matrix(y_test, y_pred)
    cm_perc = cm / cm.sum()
    
    labels = [f"{v}\n({p:.1%})" for v, p in zip(cm.flatten(), cm_perc.flatten())]
    labels = np.asarray(labels).reshape(2, 2)
    
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=labels, fmt='', cmap='Blues', cbar=False,
                xticklabels=['Benigno (0)', 'Maligno (1)'],
                yticklabels=['Benigno (0)', 'Maligno (1)'])
    plt.xlabel('Predicción')
    plt.ylabel('Real')
    plt.title('Matriz de Confusión')
    
    text_cm = f"La matriz de confusión revela los valores absolutos y porcentajes de clasificación. Se minimizan los falsos negativos (casos malignos clasificados como benignos, {cm[1,0]}), lo que es el error más costoso en diagnóstico oncológico."
    save_plot_and_text("confusion_matrix_clinical", text_cm)
    
    print("Visualizaciones clínicas generadas exitosamente.")

if __name__ == "__main__":
    main()
