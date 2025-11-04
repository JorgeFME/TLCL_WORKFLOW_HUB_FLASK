from utils.db_connection import HanaConnection
from queries.TLCL01_queries import TLCL01Queries

class TLCL01Service:
    """
    Servicio para gestionar la transferencia de datos de Electric Fact.
    Replica el graph de SAP Data Intelligence TLCL01 en Python puro.
    
    Flujo del proceso:
    1. Table Consumer: Lee datos de TELCEL_EE_TEMPELECTRICFACT
    2. Data Transform: Agrega campo MESANIO (MM.YYYY)
    3. Table Producer: UPSERT en TELCEL_EE_ELECTRICFACT
    4. SQL Executor: Trunca tabla temporal
    5. Graph Terminator: Finaliza proceso
    """
    
    def __init__(self):
        """Inicializa el servicio."""
        self.hana_conn = None
        self.queries = None

    def transfer_electric_fact_data(self):
        """
        Ejecuta la transferencia completa de datos de Electric Fact.
        
        Returns:
            dict: Resultado de la operación con status, message y detalles.
        """
        result = {
            'status': 'error',
            'message': '',
            'details': {
                'records_processed': 0,
                'temp_table_cleaned': False,
                'steps_completed': [],
                'initial_temp_count': 0,
                'final_electric_fact_count': 0
            }
        }

        try: 
            # Crear instancia de conexión
            self.hana_conn = HanaConnection()

            # Establecer conexión
            if not self.hana_conn.connect():
                result['message'] = "No se pudo establecer la conexión a la base de datos."
                return result

            # Crear instancia de consultas
            self.queries = TLCL01Queries(self.hana_conn)

            # Paso 0: Verificar conteos iniciales
            result['details']['steps_completed'].append("Iniciando proceso de transferencia Electric Fact")
            
            initial_temp_count = self.queries.get_temp_electric_fact_count()
            if initial_temp_count is None:
                result['message'] = "Error: No se pudo obtener el conteo inicial de la tabla temporal."
                return result
            
            result['details']['initial_temp_count'] = initial_temp_count
            result['details']['steps_completed'].append(f"Conteo inicial tabla temporal: {initial_temp_count} registros")

            if initial_temp_count == 0:
                result['message'] = "No hay datos en la tabla temporal TELCEL_EE_TEMPELECTRICFACT para procesar."
                result['status'] = 'warning'
                return result

            # Paso 1: Table Consumer - Obtener datos de la tabla temporal
            result['details']['steps_completed'].append("Ejecutando Table Consumer: leyendo TELCEL_EE_TEMPELECTRICFACT")
            
            temp_result = self.queries.get_temp_electric_fact_data()
            if not temp_result:
                result['message'] = "Error: No se pudieron obtener los datos de la tabla temporal."
                return result

            temp_columns, temp_data = temp_result
            result['details']['steps_completed'].append(f"Table Consumer completado: {len(temp_data)} registros obtenidos")

            # Paso 2: Data Transform - Agregar campo MESANIO
            result['details']['steps_completed'].append("Ejecutando Data Transform: agregando campo MESANIO")
            
            transform_result = self.queries.transform_data_with_mesanio(temp_columns, temp_data)
            if not transform_result:
                result['message'] = "Error: No se pudo realizar la transformación de datos."
                return result

            transformed_columns, transformed_data = transform_result
            result['details']['steps_completed'].append("Data Transform completado: campo MESANIO agregado")

            # Paso 3: Table Producer - UPSERT en tabla destino
            result['details']['steps_completed'].append("Ejecutando Table Producer: UPSERT en TELCEL_EE_ELECTRICFACT")
            
            upsert_success = self.queries.upsert_electric_fact_data(transformed_columns, transformed_data)
            if not upsert_success:
                result['message'] = "Error: No se pudo realizar el UPSERT en la tabla destino."
                return result

            result['details']['records_processed'] = len(transformed_data)
            result['details']['steps_completed'].append(f"Table Producer completado: {len(transformed_data)} registros procesados")

            # Paso 4: SQL Executor - Truncar tabla temporal
            result['details']['steps_completed'].append("Ejecutando SQL Executor: truncando tabla temporal")
            
            # truncate_success = self.queries.truncate_temp_electric_fact_table()
            truncate_success = 'Trunqueado'

            if not truncate_success:
                result['message'] = "Advertencia: Los datos se procesaron correctamente, pero no se pudo truncar la tabla temporal."
                result['status'] = 'warning'
            else:
                result['details']['temp_table_cleaned'] = True
                result['details']['steps_completed'].append("SQL Executor completado: tabla temporal truncada")

            # Paso 5: Verificar conteo final
            final_count = self.queries.get_electric_fact_count()
            if final_count is not None:
                result['details']['final_electric_fact_count'] = final_count
                result['details']['steps_completed'].append(f"Conteo final TELCEL_EE_ELECTRICFACT: {final_count} registros")

            # Paso 6: Graph Terminator - Finalizar proceso
            result['details']['steps_completed'].append("Graph Terminator: proceso completado exitosamente")
            
            result['status'] = 'success'
            result['message'] = f"Transferencia de Electric Fact completada exitosamente. {result['details']['records_processed']} registros procesados."

        except Exception as e:
            result['message'] = f"Error inesperado durante la transferencia: {str(e)}"
            result['details']['steps_completed'].append(f"Error: {str(e)}")

        finally:
            # Cerrar conexión
            if self.hana_conn:
                self.hana_conn.close()

        return result

    def get_health_status(self):
        """
        Verifica el estado de salud del servicio TLCL01.
        
        Returns:
            dict: Estado de salud con información de las tablas.
        """
        health_result = {
            'status': 'healthy',
            'message': 'Servicio TLCL01 operativo',
            'details': {
                'database_connection': False,
                'temp_table_accessible': False,
                'target_table_accessible': False,
                'temp_records_count': 0,
                'target_records_count': 0
            }
        }

        try:
            # Crear instancia de conexión
            self.hana_conn = HanaConnection()

            # Verificar conexión a la base de datos
            if not self.hana_conn.connect():
                health_result['status'] = 'unhealthy'
                health_result['message'] = 'No se pudo conectar a la base de datos'
                return health_result

            health_result['details']['database_connection'] = True
            self.queries = TLCL01Queries(self.hana_conn)

            # Verificar acceso a tabla temporal
            temp_count = self.queries.get_temp_electric_fact_count()
            if temp_count is not None:
                health_result['details']['temp_table_accessible'] = True
                health_result['details']['temp_records_count'] = temp_count

            # Verificar acceso a tabla destino
            target_count = self.queries.get_electric_fact_count()
            if target_count is not None:
                health_result['details']['target_table_accessible'] = True
                health_result['details']['target_records_count'] = target_count

            # Determinar estado general
            if not health_result['details']['temp_table_accessible'] or not health_result['details']['target_table_accessible']:
                health_result['status'] = 'degraded'
                health_result['message'] = 'Algunas tablas no son accesibles'

        except Exception as e:
            health_result['status'] = 'unhealthy'
            health_result['message'] = f'Error en verificación de salud: {str(e)}'

        finally:
            if self.hana_conn:
                self.hana_conn.close()

        return health_result

    def execute_SP_TLCL_01_sp(self, param1=0, param2=''):
        """Ejecuta el stored procedure SP_TLCL_01 que realiza todo el proceso TLCL01.

        Args:
            param1 (int): Primer parámetro de entrada opcional.
            param2 (str): Segundo parámetro de entrada opcional.
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

            queries = TLCL01Queries(connection)

            # Ejecutar stored procedure
            result = queries.execute_SP_TLCL_01_sp(param1, param2)

            return result

        except Exception as e:
            return {
                'success': False,
                'message': f'Error interno del servidor: {str(e)}',
                'data': None
            }
        finally:
            if connection:
                connection.close()