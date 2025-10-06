# TLCL Workflows Hub

API Flask para gestión de workflows y consultas de datos eléctricos conectada a SAP HANA Cloud.

## 📋 Descripción

Esta aplicación Flask proporciona endpoints REST para consultar datos de hechos eléctricos desde una base de datos SAP HANA Cloud. Está diseñada para funcionar tanto en desarrollo local como desplegada en SAP BTP Cloud Foundry.

## 🚀 Configuración Local

### Prerrequisitos

- Python 3.12+
- Acceso a SAP HANA Cloud
- Cloud Foundry CLI (para despliegue)

### Instalación

1. **Clonar el repositorio:**
```bash
git clone <repository-url>
cd tlcl-workflows-hub
```

2. **Crear entorno virtual:**
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac
```

3. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

4. **Configurar variables de entorno:**
Crear archivo `.env` con las credenciales de HANA:
```env
HANA_HOST=tu-hana-host.hanacloud.ondemand.com
HANA_PORT=443
HANA_USER=tu_usuario_hana
HANA_PASSWORD=tu_password_hana
HANA_SCHEMA=tu_schema_hana
```

5. **Ejecutar la aplicación:**
```bash
python app.py
```

## ☁️ Despliegue en Cloud Foundry

### 🔑 Configuración de Credenciales

**IMPORTANTE:** Cloud Foundry NO lee archivos `.env` automáticamente. Las credenciales deben configurarse como variables de entorno en la plataforma.

#### Cómo funcionan las credenciales:

- **Desarrollo Local:** `load_dotenv()` lee el archivo `.env`
- **Cloud Foundry:** `os.getenv()` obtiene variables del entorno de CF

#### Configuración por Espacios

**Para el espacio DEV:**
```bash
cf target -s DEV
cf set-env tlcl-workflows-hub-dev HANA_HOST "tu-hana-host.hanacloud.ondemand.com"
cf set-env tlcl-workflows-hub-dev HANA_PORT "443"
cf set-env tlcl-workflows-hub-dev HANA_USER "tu_usuario_hana"
cf set-env tlcl-workflows-hub-dev HANA_PASSWORD "tu_password_hana"
cf set-env tlcl-workflows-hub-dev HANA_SCHEMA "tu_schema_hana"
cf restart tlcl-workflows-hub-dev
```

**Para el espacio PRD:**
```bash
cf target -s PRD
cf set-env tlcl-workflows-hub-prd HANA_HOST "prod-hana-host.hanacloud.ondemand.com"
cf set-env tlcl-workflows-hub-prd HANA_PORT "443"
cf set-env tlcl-workflows-hub-prd HANA_USER "prod_usuario_hana"
cf set-env tlcl-workflows-hub-prd HANA_PASSWORD "prod_password_hana"
cf set-env tlcl-workflows-hub-prd HANA_SCHEMA "prod_schema_hana"
cf restart tlcl-workflows-hub-prd
```

#### Verificar Configuración

```bash
# Verificar variables configuradas
cf env tlcl-workflows-hub-dev

# Ver logs de la aplicación
cf logs tlcl-workflows-hub-dev --recent
```

### 📦 Despliegue

1. **Hacer login en Cloud Foundry:**
```bash
cf login -a https://api.cf.us10-001.hana.ondemand.com
```

2. **Seleccionar espacio y desplegar:**
```bash
cf target -s DEV
cf push
```

#### Identificación del Entorno en CI/CD

El endpoint principal (`/`) incluye información del esquema HANA para identificar el entorno:

```bash
curl http://localhost:5000/
```

Respuesta:
```json
{
  "name": "TLCL Workflows Hub",
  "version": "1.0.0",
  "env": "development",
  "hana_schema": "DEV_SCHEMA",  // Indica el entorno actual
  "description": "API para gestión de workflows de transferencia de datos",
  "status": "running"
}
```

En tu pipeline CI/CD puedes usar este campo para verificar el entorno:
- `DEV_SCHEMA` = Entorno de desarrollo
- `PRD_SCHEMA` = Entorno de producción

1. **Configuración única por espacio:** Las variables de entorno se configuran una sola vez por espacio
2. **Despliegues automáticos:** Solo requieren `cf push` (las variables persisten)
3. **Gestión de secretos:** Usar la plataforma CI/CD para gestionar credenciales por ambiente

## 🛠️ Estructura del Proyecto

```
├── app.py                 # Aplicación principal Flask
├── config.py             # Configuración y validación de variables
├── db_connection.py      # Conexión a SAP HANA
├── routes/               # Endpoints REST
├── services/             # Lógica de negocio
├── queries/              # Consultas SQL
├── .env                  # Variables locales (no se despliega)
├── .cfignore            # Archivos ignorados en CF
├── manifest.yml         # Configuración de despliegue
└── requirements.txt     # Dependencias Python
```

## 📡 API Endpoints

### 1. Información General
- **GET** `/` - Información general de la API
- **GET** `/health` - Health check general y conexión DB

### 2. Facturación Eléctrica

#### Transferir Datos
- **POST** `/api/electric-fact/transfer`
- **Descripción**: Ejecuta la transferencia completa de datos de la tabla temporal a la tabla final
- **Respuesta**:
```json
{
  "status": "success|partial_success|error",
  "message": "Descripción del resultado",
  "details": {
    "records_processed": 150,
    "temp_table_cleaned": true,
    "steps_completed": [
      "Iniciando proceso de transferencia",
      "Obtenidos 150 registros de tabla temporal",
      "..."
    ]
  }
}
```

#### Vista Previa de Datos
- **GET** `/api/electric-fact/preview?limit=5`
- **Parámetros**:
  - `limit` (opcional): Número de registros a mostrar (1-100, default: 5)
- **Respuesta**:
```json
{
  "status": "success|error",
  "message": "Descripción del resultado",
  "data": {
    "columns": ["COLUMN1", "COLUMN2", "..."],
    "rows": [
      ["value1", "value2", "..."],
      ["value1", "value2", "..."]
    ],
    "total_count": 150
  }
}
```

#### Health Check Específico
- **GET** `/api/electric-fact/health` - Verifica que el servicio esté funcionando

### 3. Códigos de Estado HTTP

- **200**: Operación exitosa
- **206**: Operación parcialmente exitosa
- **400**: Error en la solicitud o datos
- **404**: Endpoint no encontrado
- **500**: Error interno del servidor

## 💻 Ejemplos de Uso

### Con cURL

```bash
# Transferir datos
curl -X POST http://localhost:5000/api/electric-fact/transfer \
  -H "Content-Type: application/json"

# Vista previa
curl -X GET "http://localhost:5000/api/electric-fact/preview?limit=10"
```

### Con Python requests

```python
import requests

# Transferir datos
response = requests.post('http://localhost:5000/api/electric-fact/transfer')
result = response.json()

if result['status'] == 'success':
    print(f"Transferencia exitosa: {result['details']['records_processed']} registros")
else:
    print(f"Error: {result['message']}")

# Vista previa
response = requests.get('http://localhost:5000/api/electric-fact/preview?limit=5')
preview = response.json()

if preview['status'] == 'success':
    print(f"Total de registros: {preview['data']['total_count']}")
    print(f"Columnas: {preview['data']['columns']}")
```

## 🔧 Troubleshooting

### Error: Variables de entorno faltantes

**Problema:** `ValueError: Variables de entorno faltantes: HANA_HOST, HANA_USER...`

**Solución:**
1. **Local:** Verificar que existe el archivo `.env` con las credenciales
2. **Cloud Foundry:** Configurar variables con `cf set-env` y reiniciar

### Error de conexión a HANA

**Problema:** No se puede conectar a la base de datos

**Solución:**
1. Verificar credenciales en SAP HANA Cloud Central
2. Confirmar que la instancia HANA está activa
3. Revisar configuración de red/firewall

### Aplicación no inicia en CF

**Problema:** La aplicación falla al iniciar en Cloud Foundry

**Solución:**
1. Revisar logs: `cf logs tlcl-workflows-hub-dev --recent`
2. Verificar variables: `cf env tlcl-workflows-hub-dev`
3. Confirmar que todas las variables HANA están configuradas

## 📚 Documentación Adicional

- [SAP HANA Cloud Documentation](https://help.sap.com/hana_cloud)
- [Cloud Foundry Documentation](https://docs.cloudfoundry.org/)

## 🏗️ Arquitectura Escalable

El proyecto está diseñado para ser escalable:

1. **Separación por módulos**: Cada funcionalidad tiene su propio módulo de queries
2. **Servicios independientes**: La lógica de negocio está separada de las rutas
3. **Blueprints**: Cada conjunto de endpoints está en su propio blueprint
4. **Fácil extensión**: Para agregar nuevos workflows:
   - Crear nuevo archivo en `queries/`
   - Crear nuevo servicio en `services/`
   - Crear nuevas rutas en `routes/`
   - Registrar el blueprint en `app.py`

## 🚀 Configuración de Producción

### Gunicorn (Recomendado)
```bash
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

### CORS
La aplicación tiene CORS habilitado para permitir llamadas desde aplicaciones externas en diferentes dominios.

## 🔒 Seguridad

- ✅ Archivo `.env` excluido del despliegue (`.cfignore`)
- ✅ Credenciales gestionadas como variables de entorno
- ✅ No se incluyen secretos en el código fuente
- ⚠️ Usar HTTPS en producción
- ⚠️ Rotar credenciales periódicamente