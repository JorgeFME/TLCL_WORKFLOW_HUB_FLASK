"""
Aplicación Flask principal para TLCL Workflows Hub.
Proporciona endpoints para la gestión de workflows de transferencia de datos.
"""

from flask import Flask, jsonify
from flask_cors import CORS
from routes.electric_fact_routes import electric_fact_bp
from config import DB_CONFIG


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
    app.register_blueprint(electric_fact_bp)

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
                    "Facturación Electrica": {
                        "endpoints": {
                            "transfer": {
                                "method": "POST",
                                "url": "/api/electric-fact/transfer",
                                "description": "Ejecuta transferencia de datos de facturación eléctrica",
                            },
                            "preview": {
                                "method": "GET",
                                "url": "/api/electric-fact/preview",
                                "description": "Vista previa de datos temporales",
                                "parameters": "limit (opcional), format (opcional)",
                            },
                            "health_service": {
                                "method": "GET",
                                "url": "/api/electric-fact/health",
                                "description": "Estado del servicio de facturación eléctrica",
                            },
                            "health_general": {
                                "method": "GET",
                                "url": "/health",
                                "description": "Estado general de la aplicación",
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
