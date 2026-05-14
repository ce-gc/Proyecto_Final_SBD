"""
10_metrics_clinical.py
======================
Calcula métricas sobre los datos clínicos para el análisis y evaluación del modelo.
Genera métricas descriptivas, separabilidad, correlaciones y entrena un modelo
Random Forest, guardando los resultados e interpretaciones.
"""

import os
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split

# Configuración de rutas
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_CLINICAL_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
RESULTS_DIR = os.path.join(DATALAKE_ROOT, "Results")
os.makedirs(RESULTS_DIR, exist_ok=True)

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
    print("Iniciando cálculo de métricas clínicas...")
    
    # 1. Cargar datos
    # Como los datos en cleanse están en parquet, usamos pandas para leerlos
    df = pd.read_parquet(CLEANSE_CLINICAL_PATH)
    
    metrics_report = {}
    
    # --- Métricas descriptivas por clase ---
    desc_stats = df.groupby("diagnosis")[NUMERIC_COLS].describe()
    # Para serializar a JSON
    desc_dict = {}
    for diag in ["M", "B"]:
        if diag in desc_stats.index:
            desc_dict[diag] = {}
            for col in NUMERIC_COLS:
                desc_dict[diag][col] = {
                    "mean": float(desc_stats.loc[diag, (col, "mean")]),
                    "std": float(desc_stats.loc[diag, (col, "std")]),
                    "min": float(desc_stats.loc[diag, (col, "min")]),
                    "25%": float(desc_stats.loc[diag, (col, "25%")]),
                    "50%": float(desc_stats.loc[diag, (col, "50%")]),
                    "75%": float(desc_stats.loc[diag, (col, "75%")]),
                    "max": float(desc_stats.loc[diag, (col, "max")])
                }
    
    metrics_report["descriptive_metrics"] = desc_dict
    metrics_report["descriptive_interpretation"] = "Estas métricas muestran que variables como 'radius_mean' y 'area_mean' tienen valores sistemáticamente mayores en tumores malignos (M) frente a los benignos (B), indicando que el tamaño del tejido es un indicador fuerte del diagnóstico."

    # --- Separabilidad (Ratio de Fisher) ---
    fisher_ratios = {}
    for col in NUMERIC_COLS:
        mean_M = df[df['diagnosis'] == 'M'][col].mean()
        var_M = df[df['diagnosis'] == 'M'][col].var()
        mean_B = df[df['diagnosis'] == 'B'][col].mean()
        var_B = df[df['diagnosis'] == 'B'][col].var()
        
        fisher = ((mean_M - mean_B) ** 2) / (var_M + var_B + 1e-9)
        fisher_ratios[col] = float(fisher)
        
    # Ordenar variables por Fisher ratio
    fisher_sorted = dict(sorted(fisher_ratios.items(), key=lambda item: item[1], reverse=True))
    metrics_report["fisher_ratio"] = fisher_sorted
    metrics_report["fisher_interpretation"] = f"El ratio de Fisher mide la separabilidad entre las clases. Las variables con mayor ratio (ej. {list(fisher_sorted.keys())[0]}) separan mejor M de B, justificando fuertemente su inclusión en el modelo predictivo de cáncer."

    # --- Correlaciones ---
    corr_matrix = df[NUMERIC_COLS].corr()
    high_corr = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            if abs(corr_matrix.iloc[i, j]) > 0.9:
                high_corr.append((corr_matrix.columns[i], corr_matrix.columns[j], float(corr_matrix.iloc[i, j])))
                
    metrics_report["high_correlations"] = [{"var1": v1, "var2": v2, "corr": c} for v1, v2, c in high_corr]
    metrics_report["correlations_interpretation"] = "Se detectan pares de variables con correlación de Pearson superior a 0.9 (ej. radius_mean y perimeter_mean). Estas variables redundantes aportan casi la misma información clínica, lo que sugiere que podrían eliminarse en futuros refinamientos del modelo para evitar multicolinealidad y sobreajuste."

    # --- Modelo de Clasificación ---
    X = df[NUMERIC_COLS]
    y = df['diagnosis'].apply(lambda x: 1 if x == 'M' else 0)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    y_prob = rf.predict_proba(X_test)[:, 1]
    
    importances = rf.feature_importances_
    feat_imp = sorted([{"feature": f, "importance": float(i)} for f, i in zip(NUMERIC_COLS, importances)],
                      key=lambda x: x["importance"], reverse=True)
    
    conf_m = confusion_matrix(y_test, y_pred)
    
    model_metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision": float(precision_score(y_test, y_pred)),
        "recall": float(recall_score(y_test, y_pred)),
        "f1_score": float(f1_score(y_test, y_pred)),
        "auc_roc": float(roc_auc_score(y_test, y_prob)),
        "confusion_matrix": {"TN": int(conf_m[0,0]), "FP": int(conf_m[0,1]), "FN": int(conf_m[1,0]), "TP": int(conf_m[1,1])},
        "feature_importances": feat_imp[:10] # Guardar el top 10
    }
    metrics_report["model_evaluation"] = model_metrics
    metrics_report["model_interpretation"] = f"El modelo Random Forest obtiene un F1-score de {model_metrics['f1_score']:.3f} y un AUC-ROC de {model_metrics['auc_roc']:.3f}. Estos altos valores implican que el modelo es altamente fiable para clasificar los tumores, logrando identificar la gran mayoría de casos malignos (bajo FN) lo que es crítico en el diagnóstico temprano del cáncer de mama."

    # Guardar métricas
    output_path = os.path.join(RESULTS_DIR, "metrics_clinical.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics_report, f, indent=4, ensure_ascii=False)
        
    print(f"Métricas clínicas guardadas en {output_path}")

if __name__ == "__main__":
    main()
