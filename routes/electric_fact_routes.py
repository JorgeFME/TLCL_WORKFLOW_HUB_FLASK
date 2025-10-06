"""
Rutas para la gestión de facturación eléctrica.
Define los endpoints relacionados con la transferencia de datos de facturación eléctrica.
"""

from flask import Blueprint, request, jsonify
from services.electric_fact_service import ElectricFactService

# Crear blueprint para las rutas de facturación eléctrica
electric_fact_bp = Blueprint('electric_fact', __name__, url_prefix='/api/electric-fact')

@electric_fact_bp.route('/transfer', methods=['POST'])
def transfer_electric_fact_data():
    """
    Endpoint para ejecutar la transferencia de datos de facturación eléctrica.
    
    Returns:
        JSON: Resultado de la operación de transferencia.
    """
    try:
        service = ElectricFactService()
        result = service.transfer_electric_fact_data()
        
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
        return jsonify(error_result), 500

@electric_fact_bp.route('/preview', methods=['GET'])
def get_preview():
    """Obtiene una vista previa de los datos temporales."""
    try:
        # Obtener parámetros de la query string
        limit = request.args.get('limit', 5, type=int)
        format_type = request.args.get('format', 'objects', type=str)
        
        # Validar límite
        if limit < 1 or limit > 100:
            return jsonify({
                'status': 'error',
                'message': 'El parámetro limit debe estar entre 1 y 100'
            }), 400
        
        # Validar formato
        if format_type not in ['objects', 'arrays']:
            return jsonify({
                'status': 'error',
                'message': 'El parámetro format debe ser "objects" o "arrays"'
            }), 400
        
        service = ElectricFactService()
        result = service.get_temp_data_preview(limit, format_type)
        
        if result['status'] == 'success':
            return jsonify(result), 200
        else:
            return jsonify(result), 500
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error interno del servidor: {str(e)}',
            'data': {
                'records' if request.args.get('format', 'objects') == 'objects' else 'columns': [],
                'total_count': 0
            }
        }), 500

@electric_fact_bp.route('/health', methods=['GET'])
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