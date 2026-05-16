"""
15_viz_images.py
================
Genera visualizaciones para el dataset de imágenes (balance de clases, tamaño y brillo).
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Paleta de colores
COLOR_NORMAL = "#2ca02c"
COLOR_BENIGN = "#1f77b4"
COLOR_MALIGNANT = "#d62728"

DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_IMAGES_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "images")
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
    print("Generando visualizaciones de imágenes...")
    
    try:
        df = pd.read_parquet(CLEANSE_IMAGES_PATH)
    except FileNotFoundError:
        print(f"No se encontró el archivo de metadatos en {CLEANSE_IMAGES_PATH}")
        return
        
    category_col = "category" if "category" in df.columns else (
        "label" if "label" in df.columns else "class"
    )
    
    # Intentar unificar etiquetas para el color
    palette = {"normal": COLOR_NORMAL, "benign": COLOR_BENIGN, "malignant": COLOR_MALIGNANT}
    
    if category_col in df.columns:
        # 1. Gráfico de tarta con balance de clases
        plt.figure(figsize=(8, 8))
        class_counts = df[category_col].value_counts()
        
        colors = [palette.get(str(cls).lower(), "gray") for cls in class_counts.index]
        
        plt.pie(class_counts, labels=class_counts.index, autopct='%1.1f%%', startangle=90, colors=colors)
        plt.title("Balance de Clases en Dataset de Imágenes")
        
        text_pie = "El gráfico de tarta expone el balance de las clases en porcentaje. Esta vista es imprescindible para identificar desequilibrios significativos y justificar decisiones posteriores como la aplicación de oversampling o ajuste de pesos en el modelo."
        save_plot_and_text("class_balance_images", text_pie)
        
        # 2. Scatter alto x ancho coloreado por clase
        if 'width' in df.columns and 'height' in df.columns:
            plt.figure(figsize=(10, 8))
            custom_palette = {k: palette.get(str(k).lower(), "gray") for k in df[category_col].unique()}
            sns.scatterplot(x='width', y='height', hue=category_col, data=df, palette=custom_palette, alpha=0.6)
            plt.title("Dimensiones de Imágenes por Clase (Alto x Ancho)")
            plt.xlabel("Ancho (píxeles)")
            plt.ylabel("Alto (píxeles)")
            
            text_dim = "El scatter de alto por ancho revela la distribución de tamaños de imagen según clase. Ayuda a visualizar si hay diferencias sistemáticas de resolución entre clases o si existen claros outliers de tamaño que deban estandarizarse antes de entrenar redes neuronales."
            save_plot_and_text("dimensions_scatter_images", text_dim)
        
        # 3. Boxplot de brillo medio por clase
        brightness_cols = [c for c in df.columns if "brightness" in c.lower() or "intensity" in c.lower() or "mean_pixel" in c.lower()]
        if brightness_cols:
            b_col = brightness_cols[0]
            plt.figure(figsize=(8, 6))
            custom_palette = {k: palette.get(str(k).lower(), "gray") for k in df[category_col].unique()}
            sns.boxplot(x=category_col, y=b_col, hue=category_col, data=df, palette=custom_palette, legend=False)
            plt.title("Distribución de Intensidad (Brillo Medio) por Clase")
            plt.xlabel("Clase")
            plt.ylabel("Intensidad Media")
            
            text_bright = "El boxplot evalúa si existe una distribución de intensidad estadísticamente distinta entre clases. Diferencias en el brillo pueden tener interpretación médica directa o señalar artefactos sistemáticos del ecógrafo por diagnóstico."
            save_plot_and_text("intensity_boxplot_images", text_bright)
            
    print("Visualizaciones de imágenes generadas exitosamente.")

if __name__ == "__main__":
    main()
