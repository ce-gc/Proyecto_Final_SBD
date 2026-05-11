# Memoria Técnica del Proyecto

## Registro de Actividades

### Día 1: Realización de Fase 1 (Configuración y Estructura)

Se ha procedido con la implementación de la infraestructura básica del proyecto y la definición del almacenamiento.

**Hitos logrados:**
- **Implementación de la estructura de carpetas**: Se ha organizado el espacio de trabajo siguiendo estándares de ingeniería de datos.
- **Configuración del Data Lake**: Creación de un volumen persistente con una arquitectura de capas segregadas para el ciclo de vida del dato.
- **Orquestación con Docker Compose**: Se ha creado un archivo `docker-compose.yml` para estandarizar el entorno de desarrollo. Esto permite:
    - Facilitar el despliegue del proyecto en cualquier máquina.
    - Automatizar el montaje de volúmenes (específicamente el `datalake`) para asegurar la persistencia.
    - Garantizar que todas las dependencias y configuraciones de entorno sean idénticas para todos los colaboradores.

**Estructura principal de carpetas:**
```text
Proyecto_Final/
├── .devcontainer/
├── datalake/
│   ├── raw/
│   ├── cleanse/
│   └── curated/
├── docs/
├── jobs/
```

---
*(Este archivo se continuará actualizando con las siguientes fases del proyecto).*
