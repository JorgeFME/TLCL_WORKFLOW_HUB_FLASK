"""
Rutas para el proceso SIR.
Contiene los endpoints para ejecutar stored procedures y health check.
"""

from flask import Blueprint, jsonify
from services.SIR_service import SIRService

import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para SIR
sir_bp = Blueprint('SIR', __name__, url_prefix='/api/SIR')

@sir_bp.route('/execute', methods=['POST'])
def execute_sp():
    """Endpoint para ejecutar el stored procedure SP_TLCL_SIR."""
    try:
        logger.info("Iniciando ejecución de stored procedure (SIR)")
        
        # Crear instancia del servicio
        service = SIRService()
        
        # Ejecutar stored procedure
        result = service.execute_SP_TLCL_SIR_sp()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Ejecución SP SIR completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /execute (SIR): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@sir_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de salud del servicio SIR.
    
    Returns:
        JSON: Estado del servicio.
    """
    try:
        logger.info("Verificando estado de salud del servicio SIR")
        
        # Crear instancia del servicio
        service = SIRService()
        
        # Ejecutar health check
        result = service.health_check()
        
        status_code = 200 if result['success'] else 503
        
        logger.info(f"Health check SIR completado. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en health check: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error en health check: {str(e)}',
            'service': 'SIR',
            'database_connection': 'ERROR'
        }), 503

@sir_bp.route('/info', methods=['GET'])
def get_sp_info():
    """Endpoint para obtener información del stored procedure.
    
    Returns:
        JSON: Información del stored procedure.
    """
    try:
        logger.info("Obteniendo información del stored procedure")
        
        # Crear instancia del servicio
        service = SIRService()
        
        # Obtener información del SP
        result = service.get_sp_info()
        
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Información SP obtenida. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /info (SIR): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500