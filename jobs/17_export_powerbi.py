import os
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATALAKE_ROOT = os.path.join(BASE_DIR, "datalake") if not os.path.exists("/datalake") else "/datalake"

CLEANSE_CLINICAL_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
CLEANSE_IMAGES_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "images")

EXPORT_DIR = os.path.join(DATALAKE_ROOT, "powerbi")
os.makedirs(EXPORT_DIR, exist_ok=True)

print("Iniciando exportación de datos para Power BI...")

# 1. Datos Clínicos
print("Procesando datos clínicos...")
try:
    df_clinical = pd.read_parquet(CLEANSE_CLINICAL_PATH)
    if not df_clinical.empty:
        clin_num_cols = df_clinical.select_dtypes(include=[np.number]).columns.tolist()
        if 'id' in clin_num_cols: clin_num_cols.remove('id')
        
        # Calcular PCA
        X_clin = df_clinical[clin_num_cols].fillna(0)
        pca_clin = PCA(n_components=3, random_state=42)
        X_pca_clin = pca_clin.fit_transform(X_clin)
        df_clinical['Clin_PC1'] = X_pca_clin[:, 0]
        df_clinical['Clin_PC2'] = X_pca_clin[:, 1]
        df_clinical['Clin_PC3'] = X_pca_clin[:, 2]
        
        # Exportar base clínica principal
        df_clinical.to_csv(os.path.join(EXPORT_DIR, "clinical_data_full.csv"), index=False)
        
        # Feature Importance
        if 'diagnosis' in df_clinical.columns:
            rf = RandomForestClassifier(n_estimators=50, random_state=42)
            rf.fit(X_clin, df_clinical['diagnosis'])
            df_importance = pd.DataFrame({
                'Feature': clin_num_cols,
                'Importance': rf.feature_importances_
            }).sort_values(by='Importance', ascending=False)
            
            df_importance.to_csv(os.path.join(EXPORT_DIR, "clinical_feature_importance.csv"), index=False)
            
            # Radar Means
            top_5_features = df_importance['Feature'].head(5).tolist()
            scaler = StandardScaler()
            df_clinical_scaled = df_clinical.copy()
            df_clinical_scaled[top_5_features] = scaler.fit_transform(df_clinical[top_5_features])
            
            radar_means = df_clinical_scaled.groupby('diagnosis')[top_5_features].mean().reset_index()
            # Derretir los datos (Unpivot) para Power BI (formato tabular largo es mejor para gráficos de radar)
            radar_melted = radar_means.melt(id_vars=['diagnosis'], value_vars=top_5_features, var_name='Feature', value_name='Mean_Scaled_Value')
            radar_melted.to_csv(os.path.join(EXPORT_DIR, "clinical_radar_means.csv"), index=False)
            
    print("Datos clínicos exportados con éxito.")
except Exception as e:
    print(f"Error procesando clínicos: {e}")

# 2. Datos Genómicos
print("Procesando datos genómicos...")
try:
    df_genomics = pd.read_parquet(CLEANSE_GENOMICS_PATH)
    if not df_genomics.empty:
        excluded_cols = ["age_at_diagnosis", "survival_months", "recurrence_free_months", "patient_id", "id", "country", "cluster", "anomaly_label"]
        expression_cols = [c for c in df_genomics.select_dtypes(include=[np.number]).columns if c not in excluded_cols]
        X_gen = df_genomics[expression_cols].fillna(0)
        
        # PCA 3D
        pca_3 = PCA(n_components=3, random_state=42)
        X_pca_3 = pca_3.fit_transform(X_gen)
        df_genomics['PC1'] = X_pca_3[:, 0]
        df_genomics['PC2'] = X_pca_3[:, 1]
        df_genomics['PC3'] = X_pca_3[:, 2]
        
        # KMeans
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df_genomics['cluster'] = kmeans.fit_predict(X_pca_3[:, :2])
        df_genomics['cluster'] = df_genomics['cluster'].astype(str)
        
        # Exportar
        df_genomics.to_csv(os.path.join(EXPORT_DIR, "genomics_data_full.csv"), index=False)
    print("Datos genómicos exportados con éxito.")
except Exception as e:
    print(f"Error procesando genómicos: {e}")

# 3. Datos Imágenes
print("Procesando datos de imágenes...")
try:
    df_images = pd.read_parquet(CLEANSE_IMAGES_PATH)
    if not df_images.empty:
        df_images.to_csv(os.path.join(EXPORT_DIR, "images_data_full.csv"), index=False)
    print("Datos de imágenes exportados con éxito.")
except Exception as e:
    print(f"Error procesando imágenes: {e}")

print(f"\\n¡Exportación completada! Los archivos CSV están disponibles en: {EXPORT_DIR}")
