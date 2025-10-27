"""
Aplicación Flask principal para TLCL Workflows Hub.
Proporciona endpoints para la gestión de workflows de transferencia de datos.
"""

from flask import Flask, jsonify
from flask_cors import CORS
from routes.TLCL01_routes import tlcl01_bp
from routes.TLCL02_routes import TLCL02_bp
from routes.TLCL03_routes import TLCL03_bp
from routes.TLCL04_routes import tlcl04_bp
from routes.SIR_routes import sir_bp
from routes.COBCEN_routes import COBCEN_bp
from utils.config import DB_CONFIG


def create_app():
    """
    Factory function para crear la aplicación Flask.

    Returns:
        Flask: Instancia de la aplicación Flask configurada.
    """
    app = Flask(__name__)

    # Configuración básica
    app.config["JSON_SORT_KEYS"] = False
    app.json.sort_keys = False
    app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
    app.config["JSONIFY_MIMETYPE"] = "application/json"

    # Habilitar CORS para permitir llamadas desde aplicaciones externas
    CORS(app)

    # Registrar blueprints
    app.register_blueprint(tlcl01_bp)
    app.register_blueprint(COBCEN_bp)
    app.register_blueprint(TLCL02_bp)
    app.register_blueprint(TLCL03_bp)
    app.register_blueprint(tlcl04_bp)
    app.register_blueprint(sir_bp)


    # Ruta raíz para información general de la API
    @app.route("/")
    def index():
        """Endpoint raíz con información de la API."""
        return jsonify(
            {
                "name": "TLCL Workflows Hub",
                "version": "1.0.0",
                'env': 'development',
                "hana_schema": DB_CONFIG['schema'],
                "description": "API para gestión de workflows de transferencia de datos",
                "workflows": {
                    "TLCL01: Electric Fact": {
                        "endpoints": {
                            "transfer": {
                                "method": "POST",
                                "url": "/api/TLCL01/transfer",
                                "description": "Ejecuta transferencia de datos de Electric Fact con transformación MESANIO",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/TLCL01/health",
                                "description": "Estado del servicio de Electric Fact",
                            },
                            "status": {
                                "method": "GET",
                                "url": "/api/TLCL01/status",
                                "description": "Información general del proceso TLCL01",
                            },
                        }
                    },
                    "TLCL02: KPI": {
                        "endpoints": {
                            "transfer": {
                                "method": "POST",
                                "url": "/api/TLCL02/transfer",
                                "description": "Ejecuta transferencia de datos de KPI",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/TLCL02/health",
                                "description": "Estado del servicio de KPI",
                            },
                        }
                    },
                    "TLCL03: Huawei Counters": {
                        "endpoints": {
                            "transfer": {
                                "method": "POST",
                                "url": "/api/TLCL03/transfer",
                                "description": "Ejecuta transferencia de datos de Huawei Counters",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/TLCL03/health",
                                "description": "Estado del servicio de Huawei Counters",
                            },
                        }
                    },
                    "TLCL04: Ericsson Counters": {
                        "endpoints": {
                            "transfer": {
                                "method": "POST",
                                "url": "/api/TLCL04/transfer",
                                "description": "Ejecuta transferencia de datos de Ericsson Counters con SQL Executor inicial",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/TLCL04/health",
                                "description": "Estado del servicio de Ericsson Counters",
                            },
                            "status": {
                                "method": "GET",
                                "url": "/api/TLCL04/status",
                                "description": "Información general del proceso TLCL04",
                            },
                        }
                    },
                    "SIR: Stored Procedure Test": {
                        "endpoints": {
                            "execute": {
                                "method": "POST",
                                "url": "/api/SIR/execute",
                                "description": "Ejecuta stored procedure SALUDO_SENCILLO",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/SIR/health",
                                "description": "Estado del servicio SIR",
                            },
                            "info": {
                                "method": "GET",
                                "url": "/api/SIR/info",
                                "description": "Información del stored procedure",
                            },
                        }
                    },
                    "COBCEN": {
                        "endpoints": {
                            "merge": {
                                "method": "POST",
                                "url": "/api/COBCEN/merge",
                                "description": "Ejecuta script COBCEN (MERGE en múltiples tablas)",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/COBCEN/health",
                                "description": "Estado del servicio COBCEN",
                            },
                        }
                    },
                },
                "status": "running",
            }
        )

    # Ruta de health check general
    @app.route("/health")
    def health():
        """Health check general de la aplicación."""
        return jsonify(
            {
                "status": "healthy",
                "message": "TLCL Workflows Hub API is running",
                "version": "1.0.0",
            }
        )

    # Manejo de errores 404
    @app.errorhandler(404)
    def not_found(error):
        """Manejo de errores 404."""
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Endpoint no encontrado",
                    "error_code": 404,
                }
            ),
            404,
        )

    # Manejo de errores 500
    @app.errorhandler(500)
    def internal_error(error):
        """Manejo de errores 500."""
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Error interno del servidor",
                    "error_code": 500,
                }
            ),
            500,
        )

    return app


# Crear la aplicación
app = create_app()

if __name__ == "__main__":
    # Ejecutar en modo desarrollo
    app.run(debug=True, host="0.0.0.0", port=5000)
