"""
Rutas para el proceso COBCEN.
Contiene los endpoints para ejecutar el stored procedure SP_TLCL_COBCEN y health.
"""

from flask import Blueprint, jsonify, request
from services.COBCEN_service import COBCENService

import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para COBCEN
COBCEN_bp = Blueprint('COBCEN', __name__, url_prefix='/api/COBCEN')

@COBCEN_bp.route('/execute', methods=['GET'])
def execute_cobcen_info():
    """Endpoint informativo para el stored procedure COBCEN."""
    return jsonify({
        "endpoint": "/api/COBCEN/execute",
        "method": "POST",
        "description": "Ejecuta el stored procedure SP_TLCL_COBCEN",
        "parameters": {
            "param1": {
                "type": "integer",
                "description": "Primer parámetro de entrada (opcional, default: 0)",
                "default": 0
            },
            "param2": {
                "type": "string", 
                "description": "Segundo parámetro de entrada (opcional, default: \"\")",
                "default": ""
            }
        },
        "example_request": {
            "method": "POST",
            "url": "/api/COBCEN/execute",
            "body": {
                "param1": 1,
                "param2": "test"
            }
        },
        "example_response": {
            "success": True,
            "message": "Procedimiento COBCEN ejecutado correctamente",
            "data": {
                "flag": 1,
                "mensaje_error": "Procedimiento ejecutado correctamente",
                "response": [1, "Procedimiento ejecutado correctamente"]
            }
        },
        "note": "Esta es una página informativa. Para ejecutar el stored procedure, usa el método POST."
    })

@COBCEN_bp.route('/execute', methods=['POST'])
def execute_cobcen_sp():
    """Endpoint para ejecutar el stored procedure SP_TLCL_COBCEN."""
    try:
        logger.info("Iniciando ejecución de stored procedure SP_TLCL_COBCEN")
        
        # Obtener parámetros del request body
        data = request.get_json() or {}
        param1 = data.get('param1', 0)
        param2 = data.get('param2', '')
        
        logger.info(f"Parámetros recibidos - param1: {param1}, param2: {param2}")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar stored procedure
        result = service.execute_SP_TLCL_COBCEN_sp(param1, param2)
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Ejecución SP_TLCL_COBCEN completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /execute (COBCEN): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@COBCEN_bp.route('/merge', methods=['POST'])
def run_cobcen_merge():
    """Endpoint legacy para ejecutar el proceso COBCEN (ahora usa stored procedure).
    Mantenido para compatibilidad hacia atrás."""
    try:
        logger.info("Iniciando ejecución legacy de COBCEN (ahora usa stored procedure)")
        logger.warning("Endpoint /merge es legacy. Use /execute en su lugar.")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar método legacy que internamente usa el stored procedure
        result = service.run_cobcen_merge()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Ejecución legacy COBCEN completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /merge (COBCEN): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@COBCEN_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de salud del servicio COBCEN.
    
    Returns:
        JSON: Estado del servicio.
    """
    try:
        logger.info("Verificando estado de salud del servicio COBCEN")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar health check
        result = service.health_check()
        
        status_code = 200 if result['success'] else 503
        
        logger.info(f"Health check COBCEN completado. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en health check: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error en health check: {str(e)}',
            'service': 'COBCEN',
            'database_connection': 'ERROR'
        }), 503