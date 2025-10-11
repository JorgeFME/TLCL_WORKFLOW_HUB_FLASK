"""
Servicio para gestionar las operaciones del proceso SITE.
Contiene la lógica de negocio para las consultas de ELCEL_EE_SITE.
"""

import logging
from utils.db_connection import HanaConnection
from queries.COBCEN_queries import COBCENQueries

class COBCENService:
    """Servicio para gestionar las operaciones del proceso COBCEN."""
    
    def __init__(self):
        """Inicializa el servicio SITE."""
        self.logger = logging.getLogger(__name__)
    
    # Eliminado: método de preview get_site_data no utilizado
    
    def health_check(self):
        """Verifica el estado de salud del servicio COBCEN.
        
        Returns:
            dict: Estado del servicio.
        """
        try:
            connection = HanaConnection()
            if connection.connect():
                connection.close()
                return {
                    'success': True,
                    'message': 'Servicio COBCEN funcionando correctamente',
                    'service': 'COBCEN',
                    'database_connection': 'OK'
                }
            else:
                return {
                    'success': False,
                    'message': 'Error de conexión a la base de datos',
                    'service': 'COBCEN',
                    'database_connection': 'ERROR'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error en health check: {str(e)}',
                'service': 'COBCEN',
                'database_connection': 'ERROR'
            }

    def run_cobcen_merge(self):
        """Ejecuta el script de MERGE de COBCEN definido en queryCobcen.sql.

        Returns:
            dict: Resumen de la ejecución del script.
        """
        connection = None
        try:
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }

            queries = COBCENQueries(connection)
            exec_result = queries.run_cobcen_sql_script()
            return exec_result
        except Exception as e:
            self.logger.error(f"Error en run_cobcen_merge: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()