"""
12_metrics_images.py
====================
Calcula métricas a partir de los metadatos de las imágenes.
Genera información sobre el balance de clases, dimensiones de las imágenes,
estadísticas de píxeles y el ratio de aspecto.
"""

import os
import json
import numpy as np
import pandas as pd

# Configuración de rutas
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_IMAGES_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "images")
RESULTS_DIR = os.path.join(DATALAKE_ROOT, "Results")
os.makedirs(RESULTS_DIR, exist_ok=True)

def main():
    print("Iniciando cálculo de métricas de imágenes...")
    
    # Intentar leer desde cleanse (donde están los metadatos limpios)
    try:
        df = pd.read_parquet(CLEANSE_IMAGES_PATH)
    except FileNotFoundError:
        print(f"No se encontró el archivo en {CLEANSE_IMAGES_PATH}.")
        return
        
    metrics_report = {}
    
    # Categoría o etiqueta principal
    category_col = "category" if "category" in df.columns else (
        "label" if "label" in df.columns else "class"
    )
    
    # --- Balance de clases ---
    if category_col in df.columns:
        counts = df[category_col].value_counts().to_dict()
        total_images = len(df)
        percentages = {k: float((v / total_images) * 100) for k, v in counts.items()}
        
        metrics_report["class_balance"] = {
            "counts": {str(k): int(v) for k, v in counts.items()},
            "percentages": percentages
        }
        
        metrics_report["balance_interpretation"] = "El balance de clases muestra el porcentaje de imágenes normales, benignas y malignas. Un desbalance significativo (por ejemplo, muchas más normales que malignas) puede causar que un futuro modelo de visión computacional prediga la clase mayoritaria en exceso, por lo que podría ser necesario aplicar técnicas como Data Augmentation o class weights."
        
    # --- Dimensiones ---
    dims = ['width', 'height']
    available_dims = [d for d in dims if d in df.columns]
    if available_dims and category_col in df.columns:
        dim_stats = df.groupby(category_col)[available_dims].agg(['mean', 'std', 'min', 'max'])
        dim_dict = {}
        for cat in dim_stats.index:
            dim_dict[str(cat)] = {}
            for d in available_dims:
                dim_dict[str(cat)][d] = {
                    "mean": float(dim_stats.loc[cat, (d, 'mean')]),
                    "std": float(dim_stats.loc[cat, (d, 'std')]),
                    "min": float(dim_stats.loc[cat, (d, 'min')]),
                    "max": float(dim_stats.loc[cat, (d, 'max')])
                }
        metrics_report["dimensions_stats"] = dim_dict
        metrics_report["dimensions_interpretation"] = "Las estadísticas de dimensiones (ancho y alto) por clase revelan cuánta variabilidad existe en el dataset. Si la desviación estándar es alta o los máximos difieren mucho de los mínimos, significa que las ecografías o biopsias fueron capturadas en distintas resoluciones, lo que hará estricto e indispensable un redimensionamiento (resize/crop) antes de entrenar un CNN."

    # --- Estadísticas de píxeles (brillo medio) ---
    brightness_cols = [c for c in df.columns if "brightness" in c.lower() or "intensity" in c.lower() or "mean_pixel" in c.lower()]
    if brightness_cols and category_col in df.columns:
        b_col = brightness_cols[0]
        b_stats = df.groupby(category_col)[b_col].agg(['mean', 'std'])
        b_dict = {}
        for cat in b_stats.index:
            b_dict[str(cat)] = {
                "mean": float(b_stats.loc[cat, 'mean']),
                "std": float(b_stats.loc[cat, 'std'])
            }
        metrics_report["brightness_stats"] = b_dict
        metrics_report["brightness_interpretation"] = "Analizar el brillo o intensidad promedio es útil para observar si los tejidos malignos aparecen consistentemente más hipoecóicos (oscuros) o hiperecoicos en las imágenes médicas, lo cual aporta valor predictivo e interpretativo clínico sin haber entrenado la red neuronal todavía."

    # --- Ratio aspecto ---
    if 'width' in df.columns and 'height' in df.columns:
        df['aspect_ratio'] = df['width'] / df['height']
        
        ar_stats = {
            "mean": float(df['aspect_ratio'].mean()),
            "std": float(df['aspect_ratio'].std()),
            "min": float(df['aspect_ratio'].min()),
            "max": float(df['aspect_ratio'].max())
        }
        
        # Outliers de ratio de aspecto (ej. > 2.0 o < 0.5)
        outliers = int(((df['aspect_ratio'] > 2.0) | (df['aspect_ratio'] < 0.5)).sum())
        
        metrics_report["aspect_ratio"] = {
            "stats": ar_stats,
            "outliers_count_extreme": outliers
        }
        metrics_report["aspect_ratio_interpretation"] = f"El aspect ratio (ancho/alto) por imagen indica el formato de la captura. Se detectaron {outliers} imágenes extremadamente alargadas o achatadas. Estos outliers pueden ser recortes defectuosos, ruido o errores de etiquetado, y podrían deformarse gravemente al pasarlos por una red si no se aplica padding."

    # Guardar métricas
    output_path = os.path.join(RESULTS_DIR, "metrics_images.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics_report, f, indent=4, ensure_ascii=False)
        
    print(f"Métricas de imágenes guardadas en {output_path}")

if __name__ == "__main__":
    main()
