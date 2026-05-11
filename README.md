# Proyecto Final SBD

Proyecto de Ingeniería de Datos para el procesamiento de datasets genómicos de cáncer de mama en África.

## Estructura del Proyecto

- `datalake/`: Almacenamiento local de datos organizado en capas (raw, cleanse, curated).
- `jobs/`: Scripts de procesamiento y tareas programadas.
- `docs/`: Documentación adicional.
- `download_dataset.py`: Script para descargar el dataset desde Hugging Face.
- `test_datalake.py`: Script de prueba para verificar el montaje del datalake.

## Configuración

### Docker
El proyecto está configurado para ejecutarse en un entorno de contenedores. Para iniciar el entorno:
```bash
docker-compose up -d
```

### Requisitos
Si prefieres ejecutarlo localmente:
```bash
pip install -r requirements.txt
```
