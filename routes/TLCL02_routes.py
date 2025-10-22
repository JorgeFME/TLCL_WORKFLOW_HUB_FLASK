from flask import Blueprint, request, jsonify
from services.TLCL02_service import TLCL02Service

TLCL02_bp = Blueprint('TLCL02', __name__, url_prefix='/api/TLCL02')

@TLCL02_bp.route('/transfer', methods=['POST'])
def transfer_kpi_data():
    """
    Endpoint para ejecutar la transferencia de datos de KPI.
    
    Returns:
        JSON: Resultado de la operación de transferencia.
    """
    try:
        service = TLCL02Service()
        result = service.transfer_kpi_data()
        
        # Determinar el código de estado HTTP basado en el resultado
        if result['status'] == 'success':
            status_code = 200
        elif result['status'] == 'partial_success':
            status_code = 206  # Partial Content
        else:
            status_code = 400  # Bad Request
        
        return jsonify(result), status_code
        
    except Exception as e:
        error_result = {
            'status': 'error',
            'message': f'Error interno del servidor: {str(e)}',
            'details': {
                'records_processed': 0,
                'temp_table_cleaned': False,
                'steps_completed': []
            }
        }

@TLCL02_bp.route('/health', methods=['GET'])
def health_check():
    """
    Endpoint de health check para verificar que el servicio está funcionando.
    
    Returns:
        JSON: Estado del servicio.
    """
    return jsonify({
        'status': 'success',
        'message': 'Electric Fact service is running',
        'service': 'electric-fact',
        'version': '1.0.0'
    }), 200