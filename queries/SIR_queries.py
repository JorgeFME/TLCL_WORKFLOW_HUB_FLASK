"""
Módulo para gestionar las consultas SQL específicas del proceso SIR.
Incluye utilidades para ejecutar stored procedures relacionados con SIR.
"""

import logging
from utils.config import DB_CONFIG

class SIRQueries:
    """Clase para gestionar las consultas del proceso SIR."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión a la base de datos.
        
        Args:
            connection: Objeto de conexión a la base de datos HANA.
        """
        self.connection = connection
        self.logger = logging.getLogger(__name__)
    
    def execute_SP_TLCL_SIR_sp(self, param1=0, param2=''):
        """Ejecuta el stored procedure SP_TLCL_SIR.
        
        Returns:
            dict: Resultado de la ejecución del stored procedure.
        """
        try:
            self.logger.info("Ejecutando stored procedure SP_TLCL_SIR")
            
            cursor = self.connection.cursor
            
            # Usar el esquema desde la configuración
            schema = DB_CONFIG['schema']
            
            # Llamada directa igual que en DBeaver
            cursor.execute(f"CALL {schema}.SP_TLCL_SIR(?, ?)", (param1, param2))
            
            
            # Intentar obtener resultado si el SP devuelve algo
            try:
                result = cursor.fetchone()
                if result:
                    # El SP devuelve un flag y un mensaje
                    flag = result[0] if len(result) > 0 else None
                    mensaje_error = result[1] if len(result) > 1 else None
                    
                    self.logger.info(f"Respuesta del SP - Flag: {flag}, Mensaje: {mensaje_error}")
                    
                    # Determinar si fue exitoso basado en el flag
                    success = flag == 1 if flag is not None else True
                    
                    return {
                        'success': success,
                        'message': 'Stored procedure ejecutado exitosamente' if success else 'Error en stored procedure',
                        'data': {
                            'flag': flag,
                            'mensaje_error': mensaje_error,
                            'respuesta_completa': result
                        }
                    }
                else:
                    # No hay resultado
                    self.logger.info("SP ejecutado sin resultado")
                    return {
                        'success': True,
                        'message': 'Stored procedure ejecutado correctamente',
                        'data': {
                            'flag': None,
                            'mensaje_error': 'Sin respuesta del SP'
                        }
                    }
            except Exception as e:
                # Si el SP no devuelve result set, es normal
                if "No result set" in str(e):
                    self.logger.info("SP ejecutado correctamente (sin result set)")
                    return {
                        'success': True,
                        'message': 'Stored procedure ejecutado correctamente',
                        'data': {
                            'flag': None,
                            'mensaje_error': 'SP ejecutado sin devolver datos'
                        }
                    }
                else:
                    raise e
            
        except Exception as e:
            self.logger.error(f"Error al ejecutar stored procedure: {str(e)}")
            return {
                'success': False,
                'message': f"Error al ejecutar stored procedure: {str(e)}",
                'data': None
            } 

    def get_sp_info(self):
        """Obtiene información sobre el stored procedure utilizado.
        
        Returns:
            dict: Información del stored procedure.
        """
        try:
            self.logger.info("Obteniendo información del stored procedure")
            
            # Crear cursor
            cursor = self.connection.cursor
            
            # Consultar información del stored procedure
            cursor.execute("""
                SELECT 
                    PROCEDURE_NAME, 
                    SCHEMA_NAME, 
                    PROCEDURE_TYPE, 
                    CREATE_TIME 
                FROM SYS.PROCEDURES 
                WHERE PROCEDURE_NAME = 'SP_TLCL_SIR'
            """)
            
            # Obtener resultados
            result = cursor.fetchone()
            
            if result:
                sp_info = {
                    'name': result[0],
                    'schema': result[1],
                    'type': result[2],
                    'create_time': result[3].isoformat() if result[3] else None
                }
                
                return {
                    'success': True,
                    'message': 'Información del stored procedure obtenida exitosamente',
                    'data': sp_info
                }
            else:
                return {
                    'success': False,
                    'message': 'No se encontró información del stored procedure',
                    'data': None
                }
                
        except Exception as e:
            self.logger.error(f"Error al obtener información del stored procedure: {str(e)}")
            return {
                'success': False,
                'message': f"Error al obtener información del stored procedure: {str(e)}",
                'data': None
            }