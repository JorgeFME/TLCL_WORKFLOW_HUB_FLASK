from flask import Blueprint, jsonify, request
from services.TLCL01_service import TLCL01Service

# Crear el blueprint para TLCL01
tlcl01_bp = Blueprint('tlcl01', __name__, url_prefix='/api/TLCL01')

@tlcl01_bp.route('/transfer', methods=['POST'])
def transfer_electric_fact():
    """
    Endpoint LEGADO: Ahora se redirige a ejecutar el SP TLCL_01.
    
    Nota: Se mantiene por compatibilidad, pero se recomienda usar
    POST /api/TLCL01/execute.
    """
    try:
        service = TLCL01Service()
        body = request.get_json() or {}
        param1 = body.get('param1', 0)
        param2 = body.get('param2', '')
        result = service.execute_SP_TLCL_01_sp(param1, param2)

        status_code = 200 if result.get('success') else 500
        # Advertencia de uso legado
        result['legacy_notice'] = 'Este endpoint es legado. Usa POST /api/TLCL01/execute.'
        return jsonify(result), status_code

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500

@tlcl01_bp.route('/health', methods=['GET'])
def health_check():
    """
    Endpoint para verificar el estado de salud del servicio TLCL01.
    
    Verifica:
    - Conexión a la base de datos
    - Accesibilidad de tablas (temporal y destino)
    - Conteos de registros
    
    Returns:
        JSON: Estado de salud del servicio
    """
    try:
        service = TLCL01Service()
        health_result = service.get_health_status()
        
        # Determinar código de respuesta HTTP basado en el status
        if health_result['status'] == 'healthy':
            status_code = 200
        elif health_result['status'] == 'degraded':
            status_code = 206  # Partial Content
        else:
            status_code = 503  # Service Unavailable
            
        return jsonify(health_result), status_code
        
    except Exception as e:
        error_result = {
            'status': 'unhealthy',
            'message': f'Error en verificación de salud: {str(e)}',
            'details': {
                'database_connection': False,
                'temp_table_accessible': False,
                'target_table_accessible': False,
                'temp_records_count': 0,
                'target_records_count': 0
            }
        }
        return jsonify(error_result), 503

@tlcl01_bp.route('/status', methods=['GET'])
def get_status():
    """
    Endpoint para obtener información general del proceso TLCL01.
    
    Returns:
        JSON: Información sobre el proceso y las tablas involucradas
    """
    try:
        service = TLCL01Service()
        
        # Obtener información básica sin ejecutar el proceso completo
        status_info = {
            'process_name': 'TLCL01 - Electric Fact Transfer',
            'description': 'Replica del graph SAP Data Intelligence para transferencia de datos eléctricos',
            'source_table': 'TELCEL_EE_TEMPELECTRICFACT',
            'target_table': 'TELCEL_EE_ELECTRICFACT',
            'transformations': [
                'Agregación del campo MESANIO (formato MM.YYYY)',
                'Concatenación de MESFACENC y ANIOFACENC'
            ],
            'operations': [
                'Table Consumer: Lectura de datos temporales',
                'Data Transform: Transformación con MESANIO',
                'Table Producer: UPSERT en tabla destino',
                'SQL Executor: Truncado de tabla temporal',
                'Graph Terminator: Finalización del proceso'
            ]
        }
        
        # Agregar información de salud básica
        health_result = service.get_health_status()
        status_info['health_status'] = health_result['status']
        status_info['database_accessible'] = health_result['details']['database_connection']
        
        if health_result['details']['temp_table_accessible']:
            status_info['temp_records_available'] = health_result['details']['temp_records_count']
        
        if health_result['details']['target_table_accessible']:
            status_info['target_records_count'] = health_result['details']['target_records_count']
        
        return jsonify({
            'status': 'success',
            'message': 'Información del proceso TLCL01 obtenida exitosamente',
            'data': status_info
        }), 200
        
    except Exception as e:
        error_result = {
            'status': 'error',
            'message': f'Error al obtener información del proceso: {str(e)}',
            'data': None
        }
        return jsonify(error_result), 500


@tlcl01_bp.route('/execute', methods=['GET'])
def execute_info():
    """Página informativa del endpoint de ejecución del SP TLCL_01."""
    return jsonify({
        "endpoint": "/api/TLCL01/execute",
        "method": "POST",
        "description": "Ejecuta el stored procedure SP_TLCL_01 que encapsula todo TLCL01",
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
            "url": "/api/TLCL01/execute",
            "body": {"param1": 1, "param2": "test"}
        },
        "example_response": {
            "success": True,
            "message": "Stored procedure ejecutado correctamente",
            "data": {
                "flag": 1,
                "mensaje_error": "Procedimiento ejecutado correctamente"
            }
        },
        "note": "Esta es una página informativa. Para ejecutar el stored procedure, usa el método POST."
    })


@tlcl01_bp.route('/execute', methods=['POST'])
def execute_sp():
    """Ejecuta el stored procedure SP_TLCL_01, con parámetros opcionales."""
    try:
        service = TLCL01Service()
        body = request.get_json() or {}
        param1 = body.get('param1', 0)
        param2 = body.get('param2', '')
        result = service.execute_SP_TLCL_01_sp(param1, param2)

        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error interno del servidor: {str(e)}',
            'data': None
        }), 500