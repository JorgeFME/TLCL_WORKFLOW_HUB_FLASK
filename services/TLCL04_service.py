"""
Servicio para gestionar las operaciones del proceso TLCL04.
Contiene la lógica de negocio para el procesamiento de Ericsson Counters.
"""

import logging
from utils.db_connection import HanaConnection
from queries.TLCL04_queries import TLCL04Queries

class TLCL04Service:
    """Servicio para gestionar las operaciones del proceso TLCL04."""
    
    def __init__(self):
        """Inicializa el servicio TLCL04."""
        self.logger = logging.getLogger(__name__)
    
    def health_check(self):
        """Verifica el estado de salud del servicio TLCL04.
        
        Returns:
            dict: Estado del servicio.
        """
        try:
            connection = HanaConnection()
            if connection.connect():
                connection.close()
                return {
                    'success': True,
                    'message': 'Servicio TLCL04 funcionando correctamente',
                    'service': 'TLCL04',
                    'database_connection': 'OK'
                }
            else:
                return {
                    'success': False,
                    'message': 'Error de conexión a la base de datos',
                    'service': 'TLCL04',
                    'database_connection': 'ERROR'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error en health check: {str(e)}',
                'service': 'TLCL04',
                'database_connection': 'ERROR'
            }

    def transfer_ericsson_counters_data(self):
        """Ejecuta el proceso completo de transferencia de datos de Ericsson Counters.
        
        Replica el flujo del SAP Data Intelligence TLCL04:
        1. SQL Executor inicial (procesa múltiples fuentes de datos)
        2. Table Consumer (lee datos de TEMPERICSSONCOUNTERS)
        3. Data Transform (agrega campos calculados)
        4. Table Producer (UPSERT a ERICSSONCOUNTERS)
        5. SQL Executor final (trunca tabla temporal)
        6. Graph Terminator
        
        Returns:
            dict: Resultado del proceso completo.
        """
        connection = None
        try:
            self.logger.info("=== Iniciando proceso TLCL04 Ericsson Counters ===")
            
            # Establecer conexión
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }

            queries = TLCL04Queries(connection)
            
            # PASO 1: SQL Executor inicial - Procesar múltiples fuentes de datos
            self.logger.info("PASO 1: Ejecutando SQL Executor inicial")
            initial_result = queries.run_tlcl04_initial_sql()
            if not initial_result['success']:
                return {
                    'success': False,
                    'message': f'Error en SQL Executor inicial: {initial_result["message"]}',
                    'data': initial_result
                }
            
            # PASO 2: Table Consumer - Leer datos de tabla temporal
            self.logger.info("PASO 2: Table Consumer - Leyendo datos de TEMPERICSSONCOUNTERS")
            temp_data = queries.get_temp_ericsson_counters_data()
            if not temp_data:
                self.logger.warning("No se encontraron datos en la tabla temporal")
                return {
                    'success': True,
                    'message': 'Proceso completado - No hay datos para procesar',
                    'data': {
                        'initial_sql': initial_result,
                        'records_processed': 0
                    }
                }
            
            # PASO 3: Data Transform - Agregar campos calculados
            self.logger.info(f"PASO 3: Data Transform - Procesando {len(temp_data)} registros")
            transformed_data = queries.transform_and_add_date_fields(temp_data)
            
            # PASO 4: Table Producer - UPSERT a tabla final
            self.logger.info("PASO 4: Table Producer - UPSERT a ERICSSONCOUNTERS")
            upsert_result = queries.upsert_ericsson_counters(transformed_data)
            if not upsert_result['success']:
                return {
                    'success': False,
                    'message': f'Error en Table Producer: {upsert_result["message"]}',
                    'data': upsert_result
                }
            
            # PASO 5: SQL Executor final - Truncar tabla temporal
            self.logger.info("PASO 5: SQL Executor final - Truncando tabla temporal")
            truncate_result = queries.truncate_temp_table()
            if not truncate_result['success']:
                self.logger.warning(f"Advertencia al truncar tabla temporal: {truncate_result['message']}")
            
            # PASO 6: Graph Terminator - Obtener estadísticas finales
            self.logger.info("PASO 6: Graph Terminator - Obteniendo estadísticas finales")
            final_counts = queries.get_record_counts()
            
            self.logger.info("=== Proceso TLCL04 completado exitosamente ===")
            
            return {
                'success': True,
                'message': 'Transferencia de Ericsson Counters completada exitosamente',
                'data': {
                    'initial_sql': initial_result,
                    'records_processed': len(temp_data),
                    'upsert_result': upsert_result,
                    'truncate_result': truncate_result,
                    'final_counts': final_counts.get('counts', {}),
                    'process_steps': [
                        'SQL Executor inicial - Completado',
                        'Table Consumer - Completado',
                        'Data Transform - Completado',
                        'Table Producer - Completado',
                        'SQL Executor final - Completado',
                        'Graph Terminator - Completado'
                    ]
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error en transfer_ericsson_counters_data: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()

    def get_process_status(self):
        """Obtiene el estado general del proceso TLCL04.
        
        Returns:
            dict: Información del estado del proceso.
        """
        try:
            connection = HanaConnection()
            if not connection.connect():
                return {
                    'success': False,
                    'message': 'Error al conectar con la base de datos',
                    'data': None
                }

            queries = TLCL04Queries(connection)
            counts = queries.get_record_counts()
            
            return {
                'success': True,
                'message': 'Estado del proceso TLCL04',
                'data': {
                    'process_name': 'TLCL04 - Ericsson Counters',
                    'description': 'Procesamiento de datos de contadores Ericsson (5G, BB, DU, EUTRAN)',
                    'tables': {
                        'source': 'TELCEL_EE_TEMPERICSSONCOUNTERS',
                        'target': 'TELCEL_EE_ERICSSONCOUNTERS'
                    },
                    'record_counts': counts.get('counts', {}),
                    'process_flow': [
                        'SQL Executor inicial (múltiples fuentes)',
                        'Table Consumer (TEMPERICSSONCOUNTERS)',
                        'Data Transform (campos calculados)',
                        'Table Producer (ERICSSONCOUNTERS)',
                        'SQL Executor final (truncate)',
                        'Graph Terminator'
                    ]
                }
            }
        except Exception as e:
            self.logger.error(f"Error en get_process_status: {str(e)}")
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()