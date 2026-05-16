"""
16_dashboard.py
===============
Dashboard interactivo con Plotly Dash para explorar los datasets (Clínico, Genómico, Imágenes).
"""

import os
import pandas as pd
import numpy as np
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import webbrowser
import sys
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# Configuración de rutas
DATALAKE_ROOT = "/datalake" if os.path.exists("/datalake") else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datalake"
)
CLEANSE_CLINICAL_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "clinical")
CLEANSE_GENOMICS_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "genomics")
CLEANSE_IMAGES_PATH = os.path.join(DATALAKE_ROOT, "cleanse", "images")

# Cargar Datos
print("Cargando datos clínicos...")
try:
    df_clinical = pd.read_parquet(CLEANSE_CLINICAL_PATH)
    clin_num_cols = []
    df_importance = pd.DataFrame()
    top_5_features = []
    radar_data = []
    
    if not df_clinical.empty:
        # Extraer variables numéricas excluyendo 'id'
        clin_num_cols = df_clinical.select_dtypes(include=[np.number]).columns.tolist()
        if 'id' in clin_num_cols: clin_num_cols.remove('id')
        
        # PCA Clínico
        X_clin = df_clinical[clin_num_cols].fillna(0)
        pca_clin = PCA(n_components=3, random_state=42)
        X_pca_clin = pca_clin.fit_transform(X_clin)
        df_clinical['Clin_PC1'] = X_pca_clin[:, 0]
        df_clinical['Clin_PC2'] = X_pca_clin[:, 1]
        df_clinical['Clin_PC3'] = X_pca_clin[:, 2]
        
        # Feature Importance (Random Forest)
        top_5_features = clin_num_cols[:5] # Default
        if 'diagnosis' in df_clinical.columns:
            rf = RandomForestClassifier(n_estimators=50, random_state=42)
            rf.fit(X_clin, df_clinical['diagnosis'])
            df_importance = pd.DataFrame({
                'Feature': clin_num_cols,
                'Importance': rf.feature_importances_
            }).sort_values(by='Importance', ascending=False)
            top_5_features = df_importance['Feature'].head(5).tolist()
            
            # Preparar Radar Chart Data (Escalar top 5 y calcular medias por diagnosis)
            scaler = StandardScaler()
            df_clinical_scaled = df_clinical.copy()
            df_clinical_scaled[top_5_features] = scaler.fit_transform(df_clinical[top_5_features])
            
            radar_means = df_clinical_scaled.groupby('diagnosis')[top_5_features].mean().reset_index()
            for diag in radar_means['diagnosis'].unique():
                r_values = radar_means[radar_means['diagnosis'] == diag][top_5_features].values[0].tolist()
                radar_data.append(
                    go.Scatterpolar(
                        r=r_values + [r_values[0]], # Close loop
                        theta=top_5_features + [top_5_features[0]],
                        fill='toself',
                        name=f'Diagnosis: {diag}'
                    )
                )
except Exception as e:
    print(f"Error cargando clínico: {e}")
    df_clinical = pd.DataFrame()
    clin_num_cols = []
    df_importance = pd.DataFrame()
    top_5_features = []
    radar_data = []

print("Cargando datos genómicos...")
try:
    df_genomics = pd.read_parquet(CLEANSE_GENOMICS_PATH)
    # Pre-calcular PCA para genómica si hay datos
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
        
        # KMeans rápido
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df_genomics['cluster'] = kmeans.fit_predict(X_pca_3[:, :2])
        df_genomics['cluster'] = df_genomics['cluster'].astype(str)
        # Map clusters a nombres legibles (Cluster A, Cluster B, ...)
        cluster_vals = sorted(df_genomics['cluster'].unique())
        cluster_name_map = {v: f"Cluster {chr(65 + i)}" for i, v in enumerate(cluster_vals)}
        df_genomics['cluster_name'] = df_genomics['cluster'].map(cluster_name_map)
except Exception as e:
    print(f"Error cargando genómico: {e}")
    df_genomics = pd.DataFrame()

print("Cargando datos de imágenes...")
try:
    df_images = pd.read_parquet(CLEANSE_IMAGES_PATH)
except Exception as e:
    print(f"Error cargando imágenes: {e}")
    df_images = pd.DataFrame()

# Inicializar app Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], suppress_callback_exceptions=True)
app.title = "Dashboard Interactivo - Proyecto Final"

app.layout = dbc.Container([
    html.H1("Análisis de Datos Médicos Multi-Modal", className="my-4 text-center"),
    
    dbc.Tabs([
        dbc.Tab(label="Datos Clínicos", tab_id="tab-clinical"),
        dbc.Tab(label="Datos Genómicos", tab_id="tab-genomics"),
        dbc.Tab(label="Datos de Imágenes", tab_id="tab-images"),
    ], id="tabs", active_tab="tab-clinical"),
    
    html.Div(id="tab-content", className="p-4")
], fluid=True)

@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab")
)
def render_tab_content(active_tab):
    if active_tab == "tab-clinical":
        if df_clinical.empty:
            return html.P("No hay datos clínicos disponibles.")
            
        corr_matrix = df_clinical[clin_num_cols].corr()
        fig_corr = px.imshow(corr_matrix, text_auto=False, aspect="auto", title="Matriz de Correlación", color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
        
        # Feature Importance Figure
        fig_importance = px.bar(df_importance.head(10), x='Importance', y='Feature', orientation='h', title="Top 10 Variables (Random Forest)") if not df_importance.empty else go.Figure()
        fig_importance.update_layout(yaxis={'categoryorder':'total ascending'})
        
        # Radar Figure
        fig_radar = go.Figure(data=radar_data)
        fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True)), title="Radar Chart (Promedios Normalizados Top 5)")
        
        # PCA 3D Clínico
        fig_pca_clin = px.scatter_3d(df_clinical, x='Clin_PC1', y='Clin_PC2', z='Clin_PC3', color='diagnosis' if 'diagnosis' in df_clinical.columns else None, title="PCA 3D de Variables Clínicas", color_discrete_map={"M": "#d62728", "B": "#1f77b4"})
        
        # Scatter Matrix (Pairplot)
        fig_pairplot = px.scatter_matrix(df_clinical, dimensions=top_5_features, color='diagnosis' if 'diagnosis' in df_clinical.columns else None, title="Scatter Matrix (Top 5 Variables)", color_discrete_map={"M": "#d62728", "B": "#1f77b4"})
        fig_pairplot.update_traces(diagonal_visible=False)
        
        # Valores por defecto para dropdowns
        x_val = top_5_features[0] if top_5_features else (clin_num_cols[0] if clin_num_cols else None)
        y_val = top_5_features[1] if len(top_5_features) > 1 else (clin_num_cols[1] if len(clin_num_cols) > 1 else x_val)
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    html.Label("Variable X (Violin & Scatter):"),
                    dcc.Dropdown(id="clin-x", options=[{"label": c, "value": c} for c in clin_num_cols], value=x_val, clearable=False),
                ], width=4),
                dbc.Col([
                    html.Label("Variable Y (solo Scatter):"),
                    dcc.Dropdown(id="clin-y", options=[{"label": c, "value": c} for c in clin_num_cols], value=y_val, clearable=False),
                ], width=4)
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id="clin-scatter"), width=6),
                dbc.Col(dcc.Graph(id="clin-violin"), width=6),
            ]),
            html.Hr(),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_importance), width=6),
                dbc.Col(dcc.Graph(figure=fig_radar), width=6),
            ]),
            html.Hr(),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_pca_clin), width=6),
                dbc.Col(dcc.Graph(figure=fig_pairplot), width=6),
            ]),
            html.Hr(),
            html.H3("Matriz de Correlación", className="mt-4"),
            dcc.Graph(figure=fig_corr)
        ])
        
    elif active_tab == "tab-genomics":
        if df_genomics.empty:
            return html.P("No hay datos genómicos disponibles.")
            
        return html.Div([
            html.H3("Exploración de Clusters Genómicos (PCA 3D)"),
            dcc.Graph(
                figure=px.scatter_3d(df_genomics, x='PC1', y='PC2', z='PC3', color='cluster_name' if 'cluster_name' in df_genomics.columns else 'cluster', 
                                     hover_data=['country'] if 'country' in df_genomics.columns else [],
                                     title="Clusters K-Means en Espacio PCA 3D",
                                     color_discrete_sequence=px.colors.qualitative.Set1)
            ),
            html.H3("Distribución de Pacientes por País (Mapa)"),
            dcc.Graph(
                figure=(
                    px.choropleth(
                        df_genomics['country'].str.replace('_', ' ').value_counts().reset_index(name='count').rename(columns={'index': 'country', 'country': 'country_name'}),
                        locations="country_name",
                        locationmode="country names",
                        color="count",
                        hover_name="country_name",
                        color_continuous_scale="Viridis",
                        title="Distribución Geográfica de Pacientes"
                    ).update_layout(geo=dict(showframe=False, showcoastlines=True, projection_type='equirectangular'))
                    if 'country' in df_genomics.columns else go.Figure()
                )
            ),
            html.H3("Composición de Clusters por País (Barras)", className="mt-4"),
            dcc.Graph(
                figure=(
                    px.histogram(df_genomics, x='country' if 'country' in df_genomics.columns else 'cluster_name', color='cluster_name' if 'cluster_name' in df_genomics.columns else 'cluster',
                                 title="Distribución de Clusters por País", barmode='group')
                    if not df_genomics.empty else go.Figure()
                )
            )
        ])
        
    elif active_tab == "tab-images":
        if df_images.empty:
            return html.P("No hay datos de imágenes disponibles.")
            
        category_col = "category" if "category" in df_images.columns else ("label" if "label" in df_images.columns else "class")
        
        fig1 = go.Figure()
        if category_col in df_images.columns:
            counts = df_images[category_col].value_counts()
            fig1 = px.pie(values=counts.values, names=counts.index, title="Balance de Categorías")
            
        fig2 = go.Figure()
        if 'width' in df_images.columns and 'height' in df_images.columns:
            fig2 = px.scatter(df_images, x='width', y='height', color=category_col if category_col in df_images.columns else None,
                              title="Dimensiones de Imágenes", opacity=0.6)
                              
        return html.Div([
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig1), width=6),
                dbc.Col(dcc.Graph(figure=fig2), width=6),
            ])
        ])

@app.callback(
    Output("clin-scatter", "figure"),
    Input("clin-x", "value"),
    Input("clin-y", "value")
)
def update_clinical_scatter(x_col, y_col):
    if not x_col or not y_col or df_clinical.empty:
        return go.Figure()
        
    fig = px.scatter(df_clinical, x=x_col, y=y_col, color="diagnosis" if "diagnosis" in df_clinical.columns else None, 
                     title=f"Dispersión: {x_col} vs {y_col}",
                     color_discrete_map={"M": "#d62728", "B": "#1f77b4"})
    return fig

@app.callback(
    Output("clin-violin", "figure"),
    Input("clin-x", "value")
)
def update_clinical_violin(x_col):
    if not x_col or df_clinical.empty:
        return go.Figure()
    
    fig = px.violin(df_clinical, x="diagnosis" if "diagnosis" in df_clinical.columns else None, y=x_col, 
                 color="diagnosis" if "diagnosis" in df_clinical.columns else None,
                 box=True, points="all",
                 title=f"Distribución de {x_col}",
                 color_discrete_map={"M": "#d62728", "B": "#1f77b4"})
    return fig

if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8050
    local_url = f"http://localhost:{port}/"
    host_url = f"http://{host}:{port}/"
    print("\nDashboard iniciado.")
    print(f"Abre en el navegador: {local_url}")
    print(f"Si ejecutas desde otra máquina en la red, usa: {host_url}")
    print("PowerShell: Start-Process 'http://localhost:8050'")
    # Intentar abrir en el navegador por defecto (solo en ejecuciones locales)
    try:
        if sys.platform.startswith('win') or sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
            webbrowser.open(local_url)
    except Exception:
        pass
    app.run(host=host, port=port, debug=True)
