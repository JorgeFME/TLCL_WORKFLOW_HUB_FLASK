"""
Servicio para gestionar las operaciones del proceso SIR.
Contiene la lógica de negocio para ejecutar stored procedures.
"""

import logging
from utils.db_connection import HanaConnection
from queries.SIR_queries import SIRQueries

class SIRService:
    """Servicio para gestionar las operaciones del proceso SIR."""
    
    def __init__(self):
        """Inicializa el servicio SIR."""
        self.logger = logging.getLogger(__name__)
    
    def health_check(self):
        """Verifica el estado de salud del servicio SIR.
        
        Returns:
            dict: Estado del servicio.
        """
        try:
            connection = HanaConnection()
            if connection.connect():
                connection.close()
                return {
                    'success': True,
                    'message': 'Servicio SIR funcionando correctamente',
                    'service': 'SIR',
                    'database_connection': 'OK'
                }
            else:
                return {
                    'success': False,
                    'message': 'Error de conexión a la base de datos',
                    'service': 'SIR',
                    'database_connection': 'ERROR'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error en health check: {str(e)}',
                'service': 'SIR',
                'database_connection': 'ERROR'
            }

    def execute_SP_TLCL_SIR_sp(self):
        """Ejecuta el stored procedure SP_TLCL_SIR.
        
        Returns:
            dict: Resultado de la ejecución.
        """
        connection = None
        try:
            self.logger.info("=== Iniciando proceso SIR - Ejecución de SP SP_TLCL_SIR ===")
            
            # Establecer conexión
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }

            queries = SIRQueries(connection)
            
            # Ejecutar stored procedure
            result = queries.execute_SP_TLCL_SIR_sp()
            
            self.logger.info("=== Proceso SIR completado exitosamente ===")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error en execute_saludo_sp: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()

    def get_sp_info(self):
        """Obtiene información sobre el stored procedure utilizado.
        
        Returns:
            dict: Información del stored procedure.
        """
        connection = None
        try:
            self.logger.info("Obteniendo información del stored procedure")
            
            # Establecer conexión
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }

            queries = SIRQueries(connection)
            
            # Obtener información del SP
            result = queries.get_sp_info()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error en get_sp_info: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()