"""
Rutas para el proceso SITE.
Contiene los endpoints para las operaciones relacionadas con TELCEL_EE_SITE.
"""

from flask import Blueprint, request, jsonify
from services.COBCEN_service import COBCENService
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para COBCEN
COBCEN_bp = Blueprint('COBCEN', __name__, url_prefix='/api/COBCEN')

@COBCEN_bp.route('/site', methods=['POST'])
def get_site_data():
    """Endpoint para obtener datos de la tabla TELCEL_EE_SITE.
    
    Returns:
        JSON: Respuesta con los datos obtenidos o mensaje de error.
    """
    try:
        logger.info("Iniciando consulta de datos SITE")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar consulta
        result = service.get_site_data()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Consulta SITE completada. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /site: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@COBCEN_bp.route('/preview', methods=['GET'])
def preview_site_data():
    """Endpoint para previsualizar los datos de TELCEL_EE_SITE sin ejecutar operaciones.
    
    Returns:
        JSON: Vista previa de los datos disponibles.
    """
    try:
        logger.info("Iniciando preview de datos SITE")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Obtener datos para preview
        result = service.get_site_data()
        
        if result['success']:
            preview_result = {
                'success': True,
                'message': 'Preview de datos SITE generado exitosamente',
                'preview': {
                    'table_name': 'TELCEL_EE_SITE',
                    'query_type': 'SELECT TOP 1',
                    'columns': result['data']['columns'] if result['data'] else [],
                    'records': result['data']['records'] if result['data'] else [],
                    'total_records': result['data']['total_records'] if result['data'] else 0
                }
            }
        else:
            preview_result = {
                'success': False,
                'message': 'Error al generar preview de datos SITE',
                'preview': None
            }
        
        status_code = 200 if preview_result['success'] else 500
        
        logger.info(f"Preview SITE completado. Success: {preview_result['success']}")
        
        return jsonify(preview_result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /preview: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'preview': None
        }), 500

@COBCEN_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de salud del servicio SITE.
    
    Returns:
        JSON: Estado del servicio.
    """
    try:
        logger.info("Verificando estado de salud del servicio SITE")
        
        # Crear instancia del servicio
        service = COBCENService()
        
        # Ejecutar health check
        result = service.health_check()
        
        status_code = 200 if result['success'] else 503
        
        logger.info(f"Health check SITE completado. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en health check: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error en health check: {str(e)}',
            'service': 'SITE',
            'database_connection': 'ERROR'
        }), 503