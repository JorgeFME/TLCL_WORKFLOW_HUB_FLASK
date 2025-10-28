"""
Módulo para gestionar las consultas SQL específicas del proceso COBCEN.
Incluye utilidades para ejecutar el stored procedure SP_TLCL_COBCEN.
"""

import logging
from utils.config import DB_CONFIG

class COBCENQueries:
    """Clase para gestionar las consultas específicas del proceso COBCEN."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection
        self.logger = logging.getLogger(__name__)
    
    def execute_SP_TLCL_COBCEN_sp(self, param1=0, param2=''):
        """Ejecuta el stored procedure SP_TLCL_COBCEN.

        Args:
            param1 (int): Primer parámetro de entrada (opcional, default: 0)
            param2 (str): Segundo parámetro de entrada (opcional, default: '')

        Returns:
            dict: Resultado de la ejecución del stored procedure con flag y mensaje.
        """
        try:
            schema = DB_CONFIG['schema']
            cursor = self.connection.cursor
            
            # Ejecutar el stored procedure
            cursor.execute(f"CALL {schema}.SP_TLCL_COBCEN(?, ?)", (param1, param2))
            
            try:
                # Intentar obtener el resultado
                result = cursor.fetchone()
                if result:
                    flag = result[0] if len(result) > 0 else 0
                    mensaje_error = result[1] if len(result) > 1 else "Sin mensaje"
                    
                    self.logger.info(f"SP_TLCL_COBCEN ejecutado - Flag: {flag}, Mensaje: {mensaje_error}")
                    
                    # Determinar éxito basado en el flag
                    success = flag == 1
                    
                    return {
                        'success': success,
                        'message': mensaje_error if not success else "Procedimiento COBCEN ejecutado correctamente",
                        'data': {
                            'flag': flag,
                            'mensaje_error': mensaje_error,
                            'response': result
                        }
                    }
                else:
                    return {
                        'success': False,
                        'message': 'No se obtuvo respuesta del stored procedure',
                        'data': None
                    }
            except Exception as no_result_error:
                # Si no hay result set, consideramos que se ejecutó correctamente
                self.logger.info(f"SP_TLCL_COBCEN ejecutado sin result set: {str(no_result_error)}")
                return {
                    'success': True,
                    'message': 'Procedimiento COBCEN ejecutado correctamente',
                    'data': {
                        'flag': 1,
                        'mensaje_error': 'Procedimiento ejecutado correctamente',
                        'response': None
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Error ejecutando SP_TLCL_COBCEN: {str(e)}")
            return {
                'success': False,
                'message': f'Error al ejecutar stored procedure COBCEN: {str(e)}',
                'data': None
            }
        finally:
            if 'cursor' in locals():
                cursor.close()