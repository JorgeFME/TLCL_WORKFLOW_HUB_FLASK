"""
Rutas para el proceso TLCL04.
Contiene los endpoints para ejecutar el proceso de Ericsson Counters y health check.
"""

from flask import Blueprint, jsonify
from services.TLCL04_service import TLCL04Service

import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para TLCL04
tlcl04_bp = Blueprint('TLCL04', __name__, url_prefix='/api/TLCL04')

@tlcl04_bp.route('/transfer', methods=['POST'])
def transfer_ericsson_counters():
    """Endpoint para ejecutar el proceso completo de transferencia de Ericsson Counters."""
    try:
        logger.info("Iniciando transferencia de Ericsson Counters (TLCL04)")
        
        # Crear instancia del servicio
        service = TLCL04Service()
        
        # Ejecutar proceso de transferencia
        result = service.transfer_ericsson_counters_data()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Transferencia TLCL04 completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /transfer (TLCL04): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@tlcl04_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de salud del servicio TLCL04.
    
    Returns:
        JSON: Estado del servicio.
    """
    try:
        logger.info("Verificando estado de salud del servicio TLCL04")
        
        # Crear instancia del servicio
        service = TLCL04Service()
        
        # Ejecutar health check
        result = service.health_check()
        
        status_code = 200 if result['success'] else 503
        
        logger.info(f"Health check TLCL04 completado. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en health check: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error en health check: {str(e)}',
            'service': 'TLCL04',
            'database_connection': 'ERROR'
        }), 503

@tlcl04_bp.route('/status', methods=['GET'])
def get_process_status():
    """Endpoint para obtener información general del proceso TLCL04.
    
    Returns:
        JSON: Información del estado del proceso.
    """
    try:
        logger.info("Obteniendo estado del proceso TLCL04")
        
        # Crear instancia del servicio
        service = TLCL04Service()
        
        # Obtener estado del proceso
        result = service.get_process_status()
        
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Estado del proceso TLCL04 obtenido. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /status (TLCL04): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500