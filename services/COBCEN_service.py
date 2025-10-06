"""
Servicio para gestionar las operaciones del proceso SITE.
Contiene la lógica de negocio para las consultas de ELCEL_EE_SITE.
"""

import logging
from db_connection import HanaConnection
from queries.COBCEN_queries import COBCENQueries

class COBCENService:
    """Servicio para gestionar las operaciones del proceso SITE."""
    
    def __init__(self):
        """Inicializa el servicio SITE."""
        self.logger = logging.getLogger(__name__)
    
    def get_site_data(self):
        """Obtiene datos de la tabla ELCEL_EE_SITE.
        
        Returns:
            dict: Respuesta con el estado de la operación y los datos obtenidos.
        """
        connection = None
        try:
            # Establecer conexión
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }
            
            # Crear instancia de queries
            queries = COBCENQueries(connection)
            
            # Obtener datos
            result = queries.get_site_data()
            
            if result is None:
                return {
                    'success': False,
                    'message': 'Error al ejecutar la consulta',
                    'data': None
                }
            
            columns, data = result
            
            # Formatear respuesta
            formatted_data = []
            if data:
                for row in data:
                    row_dict = {}
                    for i, column in enumerate(columns):
                        row_dict[column] = row[i]
                    formatted_data.append(row_dict)
            
            return {
                'success': True,
                'message': f'Datos obtenidos exitosamente. Registros encontrados: {len(formatted_data)}',
                'data': {
                    'columns': columns,
                    'records': formatted_data,
                    'total_records': len(formatted_data)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error en get_site_data: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        
        finally:
            if connection:
                connection.close()
    
    def health_check(self):
        """Verifica el estado de salud del servicio SITE.
        
        Returns:
            dict: Estado del servicio.
        """
        try:
            connection = HanaConnection()
            if connection.connect():
                connection.close()
                return {
                    'success': True,
                    'message': 'Servicio SITE funcionando correctamente',
                    'service': 'SITE',
                    'database_connection': 'OK'
                }
            else:
                return {
                    'success': False,
                    'message': 'Error de conexión a la base de datos',
                    'service': 'SITE',
                    'database_connection': 'ERROR'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error en health check: {str(e)}',
                'service': 'SITE',
                'database_connection': 'ERROR'
            }