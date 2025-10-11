"""
Rutas para el proceso COBCEN.
Contiene los endpoints para ejecutar el script COBCEN (MERGE) y health.
"""

from flask import Blueprint, jsonify
from services.COBCEN_service import COBCENService
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para COBCEN
COBCEN_bp = Blueprint('COBCEN', __name__, url_prefix='/api/COBCEN')

@COBCEN_bp.route('/merge', methods=['POST'])
def run_cobcen_merge():
    """Endpoint para ejecutar el script COBCEN (MERGE en múltiples tablas)."""
    try:
        logger.info("Iniciando ejecución de script COBCEN")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar script de merges
        result = service.run_cobcen_merge()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Ejecución COBCEN completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /merge (COBCEN): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

# Eliminado: endpoint de preview no utilizado

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