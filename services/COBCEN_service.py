"""
Servicio para gestionar las operaciones del proceso COBCEN.
Contiene la lógica de negocio para ejecutar el stored procedure SP_TLCL_COBCEN.
"""

import logging
from utils.db_connection import HanaConnection
from queries.COBCEN_queries import COBCENQueries

class COBCENService:
    """Servicio para gestionar las operaciones del proceso COBCEN."""
    
    def __init__(self):
        """Inicializa el servicio COBCEN."""
        self.logger = logging.getLogger(__name__)
    
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

    def execute_SP_TLCL_COBCEN_sp(self, param1=0, param2=''):
        """Ejecuta el stored procedure SP_TLCL_COBCEN.

        Args:
            param1 (int): Primer parámetro de entrada (opcional, default: 0)
            param2 (str): Segundo parámetro de entrada (opcional, default: '')

        Returns:
            dict: Resultado de la ejecución del stored procedure.
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
            exec_result = queries.execute_SP_TLCL_COBCEN_sp(param1, param2)
            return exec_result
        except Exception as e:
            self.logger.error(f"Error en execute_SP_TLCL_COBCEN_sp: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()

    # Método legacy mantenido para compatibilidad hacia atrás
    def run_cobcen_merge(self):
        """Método legacy que ahora ejecuta el stored procedure SP_TLCL_COBCEN.
        Mantenido para compatibilidad hacia atrás.

        Returns:
            dict: Resultado de la ejecución del stored procedure.
        """
        self.logger.warning("Método run_cobcen_merge es legacy. Use execute_SP_TLCL_COBCEN_sp en su lugar.")
        return self.execute_SP_TLCL_COBCEN_sp()