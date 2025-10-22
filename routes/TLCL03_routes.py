"""
Rutas para el proceso COUNTERS.
Contiene los endpoints para ejecutar el script COUNTERS (MERGE) y health.
"""

from flask import Blueprint, jsonify
from services.TLCL03_service import TLCL03Service
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear blueprint para COUNTERS
TLCL03_bp = Blueprint('TLCL03', __name__, url_prefix='/api/TLCL03')

@TLCL03_bp.route('/merge', methods=['POST'])
def run_counters_merge():
    """Endpoint para ejecutar el script COUNTERS (MERGE en múltiples tablas) y transferir datos."""
    try:
        logger.info("Iniciando ejecución de script COUNTERS")
        
        # Crear instancia del servicio
        service = TLCL03Service()
        
        # Ejecutar script de merges (proceso principal)
        merge_result = service.run_counters_merge()
        
        # Inicializar respuesta combinada
        combined_result = {
            'success': merge_result['success'],
            'merge_process': merge_result,
            'transfer_process': None
        }
        
        # Si el merge fue exitoso, ejecutar la transferencia de datos
        if merge_result['success']:
            logger.info("Merge exitoso, iniciando transferencia de datos Huawei Counters")
            
            try:
                transfer_result = service.transfer_huawei_counters_data()
                combined_result['transfer_process'] = transfer_result
                
                # El éxito general depende de ambos procesos
                combined_result['success'] = merge_result['success'] and (transfer_result.get('status') == 'success')
                
                if transfer_result.get('status') == 'success':
                    logger.info("Transferencia de datos completada exitosamente")
                    combined_result['message'] = "Ambos procesos completados exitosamente: MERGE y transferencia de datos"
                else:
                    logger.warning(f"Transferencia falló: {transfer_result.get('message', 'Error desconocido')}")
                    combined_result['message'] = f"MERGE exitoso, pero transferencia falló: {transfer_result.get('message', 'Error desconocido')}"
                    
            except Exception as transfer_error:
                logger.error(f"Error durante la transferencia: {str(transfer_error)}")
                combined_result['transfer_process'] = {
                    'status': 'error',
                    'message': f'Error durante la transferencia: {str(transfer_error)}'
                }
                combined_result['success'] = False
                combined_result['message'] = f"MERGE exitoso, pero error en transferencia: {str(transfer_error)}"
        else:
            logger.warning("Merge falló, omitiendo transferencia de datos")
            combined_result['message'] = f"Proceso MERGE falló: {merge_result.get('message', 'Error desconocido')}"
        
        # Determinar código de respuesta HTTP
        status_code = 200 if combined_result['success'] else 500
        
        logger.info(f"Ejecución COUNTERS completada. Success: {combined_result['success']}")
        
        return jsonify(combined_result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /merge (COUNTERS): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'merge_process': None,
            'transfer_process': None
        }), 500

@TLCL03_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de salud del servicio COUNTERS.
    
    Returns:
        JSON: Estado del servicio.
    """
    try:
        logger.info("Verificando estado de salud del servicio COUNTERS")
        
        # Crear instancia del servicio
        service = TLCL03Service()
        
        # Ejecutar health check
        result = service.health_check()
        
        # Determinar código de respuesta HTTP
        status_code = 200 if result['success'] else 500
        
        logger.info(f"Health check COUNTERS completado. Success: {result['success']}")
        
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error en endpoint /health (COUNTERS): {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500
