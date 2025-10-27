# TLCL Workflows Hub

API Flask para gestión de workflows de transferencia y merge de datos sobre SAP HANA Cloud.

## Descripción

La aplicación ofrece endpoints REST para ejecutar procesos de negocio (p. ej., COBCEN). Adopta un enfoque híbrido para consultas SQL:
- SQL en archivos `.sql` para operaciones complejas y multi‑sentencia (MERGE, cargas, DDL/DML secuenciales).
- SQL inline en Python para consultas parametrizadas y de lectura simples.
- Una utilidad común (`utils/sql_runner.py`) normaliza la ejecución (archivos e inline), commits y manejo de errores.

## Configuración Local

Prerrequisitos:
- Python 3.12+
- Acceso a SAP HANA Cloud
- Cloud Foundry CLI (para despliegue opcional)

Instalación:
1) Clonar el repositorio
   ```bash
   git clone <repository-url>
   cd TLCL_WORKFLOW_HUB_FLASK
   ```
2) Crear entorno virtual
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # Linux/Mac
   ```
3) Instalar dependencias
   ```bash
   pip install -r requirements.txt
   ```
4) Configurar variables de entorno (.env)
   ```env
   HANA_HOST=tu-hana-host.hanacloud.ondemand.com
   HANA_PORT=443
   HANA_USER=tu_usuario_hana
   HANA_PASSWORD=tu_password_hana
   HANA_SCHEMA=tu_schema_hana
   ```
5) Ejecutar la aplicación
   ```bash
   python app.py
   ```

## Despliegue en Cloud Foundry

Credenciales:
- En Cloud Foundry, define las variables de entorno con `cf set-env` y reinicia la app.
- El endpoint raíz (`/`) incluye `hana_schema` para identificar el entorno activo.

Ejemplo rápido (DEV):
```bash
cf target -s <DEV o PRD>
cf set-env tlcl-workflows-hub-dev HANA_HOST "tu-hana-host.hanacloud.ondemand.com"
cf set-env tlcl-workflows-hub-dev HANA_PORT "443"
cf set-env tlcl-workflows-hub-dev HANA_USER "tu_usuario_hana"
cf set-env tlcl-workflows-hub-dev HANA_PASSWORD "tu_password_hana"
cf set-env tlcl-workflows-hub-dev HANA_SCHEMA "tu_schema_hana"
cf restart tlcl-workflows-hub-dev
```

Despliegue:
```bash
cf login -a https://api.cf.us10-001.hana.ondemand.com
cf target -s DEV
cf push
```

## Estructura del Proyecto

```
TLCL_WORKFLOW_HUB_FLASK/
├── app.py                      # Aplicación principal Flask
├── requirements.txt            # Dependencias del proyecto
├── README.md                   # Documentación del proyecto
├── utils/
│   ├── config.py              # Configuración de la base de datos
│   └── db_connection.py       # Gestión de conexiones HANA
├── queries/
│   ├── TLCL01_queries.py      # Consultas para Electric Fact
│   ├── TLCL02_queries.py      # Consultas para KPI
│   ├── TLCL03_queries.py      # Consultas para Huawei Counters
│   ├── TLCL04_queries.py      # Consultas para Ericsson Counters
│   ├── SIR_queries.py         # Consultas para SIR (Stored Procedure)
│   └── COBCEN_queries.py      # Consultas para COBCEN
├── services/
│   ├── TLCL01_service.py      # Lógica de negocio Electric Fact
│   ├── TLCL02_service.py      # Lógica de negocio KPI
│   ├── TLCL03_service.py      # Lógica de negocio Huawei Counters
│   ├── TLCL04_service.py      # Lógica de negocio Ericsson Counters
│   ├── SIR_service.py         # Lógica de negocio SIR (Stored Procedure)
│   └── COBCEN_service.py      # Lógica de negocio COBCEN
└── routes/
    ├── TLCL01_routes.py       # Endpoints REST Electric Fact
    ├── TLCL02_routes.py       # Endpoints REST KPI
    ├── TLCL03_routes.py       # Endpoints REST Huawei Counters
    ├── TLCL04_routes.py       # Endpoints REST Ericsson Counters
    ├── SIR_routes.py          # Endpoints REST SIR (Stored Procedure)
    └── COBCEN_routes.py       # Endpoints REST COBCEN
```

## API Endpoints

Generales:
- `GET /` — Información de la API y workflows registrados
- `GET /health` — Health check general

TLCL01 (Electric Fact):
- `POST /api/TLCL01/transfer` — Ejecuta transferencia de Electric Fact con transformación MESANIO
- `GET /api/TLCL01/health` — Estado del servicio TLCL01
- `GET /api/TLCL01/status` — Información general del proceso TLCL01

TLCL02 (KPI):
- `POST /api/TLCL02/transfer` — Ejecuta transferencia de datos de KPI
- `GET /api/TLCL02/health` — Estado del servicio TLCL02

TLCL03 (Huawei Counters):
- `POST /api/TLCL03/transfer` — Ejecuta transferencia de datos de Huawei Counters
- `GET /api/TLCL03/health` — Estado del servicio TLCL03

TLCL04 (Ericsson Counters):
- `POST /api/TLCL04/transfer` — Ejecuta transferencia de datos de Ericsson Counters con SQL Executor inicial
- `GET /api/TLCL04/health` — Estado del servicio TLCL04
- `GET /api/TLCL04/status` — Información general del proceso TLCL04

SIR (Stored Procedure):
- `POST /api/SIR/execute` — Ejecuta el stored procedure SP_TLCL_SIR
- `GET /api/SIR/health` — Estado del servicio SIR
- `GET /api/SIR/info` — Obtiene información del stored procedure

COBCEN:
- `POST /api/COBCEN/merge` — Ejecuta `queries/COBCEN_merge.sql` (MERGE secuencial)
- `GET /api/COBCEN/health` — Estado del servicio COBCEN

## Utilidad Común de SQL (SqlRunner)

Archivo: `utils/sql_runner.py`
- `execute_sql_file(path, commit_mode='end', stop_on_error=True)`
  - Limpia comentarios (`--`, `/* ... */`), divide por `;`, ejecuta secuencialmente.
  - `commit_mode='end'` confirma al final; `'per_statement'` confirma tras cada sentencia.
  - `stop_on_error=True` detiene al primer error y devuelve el índice de la sentencia.
- `execute_statements(statements, commit_mode='end', stop_on_error=True)`
  - Ejecuta una lista de sentencias inline con el mismo modelo de commits y errores.

Ejemplo (COBCEN):
```python
from utils.sql_runner import SqlRunner
runner = SqlRunner(self.connection)
res = runner.execute_sql_file(sql_path, commit_mode='end', stop_on_error=True)
```

## Cómo Crear un Nuevo Proceso (Híbrido)

1) Definir nombres y alcance
- Identifica el nombre corto del proceso (`PROCESS`, p. ej. `MYPROC`) y sus acciones (`merge`, `transfer`, `preview`).
- Decide qué va en `.sql` (multi‑sentencia) y qué queda inline (SELECTs, pequeñas operaciones con parámetros).

2) Preparar SQL de archivo (si aplica)
- Crea `queries/PROCESS_merge.sql` con sentencias separadas por `;` y ordenadas según dependencias.
- Usa comentarios `--` solo como encabezados descriptivos.

3) Capa de queries
- Crea `queries/PROCESS_queries.py` con `class PROCESSQueries`.
- Para `.sql`: importa `SqlRunner`, construye ruta absoluta y llama `execute_sql_file(...)`.
- Para inline: usa `cursor.execute(...)` o `runner.execute_statements([...])` si hay secuencia.
- Devuelve dicts con `success`, `message`, `details`.

4) Capa de servicio
- Crea `services/PROCESS_service.py` con `class PROCESSService`.
- Orquesta conexión (`HanaConnection`), invoca la capa de queries y retorna resultados estructurados.
- Incluye `health_check()` para validar conexión HANA.

5) Endpoints
- Crea `routes/PROCESS_routes.py` con `Blueprint('PROCESS', url_prefix='/api/PROCESS')`.
- Define `POST /merge` (o acción principal) que llama `PROCESSService().run_merge()`.
- Define `GET /health`.
- Opcional `GET /preview` si el proceso lo requiere.

6) Registrar en la app
- En `app.py`: importa y registra `PROCESS_bp`.
- Actualiza el índice `/` para listar los endpoints del nuevo proceso.

7) Validar
- Ejecuta `python app.py`.
- Prueba los endpoints; para scripts `.sql` revisa `details.statements_executed`.

## Buenas Prácticas

- Parámetros seguros: evita concatenación, usa parámetros del cliente HANA para inline.
- Transacciones: usa `commit_mode='end'` para consistencia; cambia a `'per_statement'` si necesitas persistencia por paso.
- Manejo de errores: registra el índice y el mensaje; decide si `stop_on_error` o continuar.
- Convenciones: nombres descriptivos (`PROCESS_merge.sql`), comentarios de bloque por sección.
- Logging: mensajes claros por inicio/fin y éxito/error.
- Pruebas: mock de HANA y modo “dry‑run” si deseas validar parsing de scripts.

## Troubleshooting

Variables faltantes:
- Verifica `.env` en local; en CF usa `cf set-env` y reinicia.

Conexión HANA:
- Revisa credenciales, instancia activa y red/firewall.

Logs en CF:
```bash
cf logs <app-name> --recent
cf env <app-name>
```

## Producción (Opcional)

Servidor WSGI recomendado:
```bash
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```