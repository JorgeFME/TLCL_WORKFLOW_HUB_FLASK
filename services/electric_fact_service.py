"""
Servicio para gestionar la transferencia de datos de facturación eléctrica.
Encapsula la lógica de negocio de la transferencia de datos de tabla temporal a tabla final.
"""

from db_connection import HanaConnection
from queries.electric_fact import ElectricFactQueries

class ElectricFactService:
    """Servicio para gestionar la transferencia de datos de facturación eléctrica."""
    
    def __init__(self):
        """Inicializa el servicio."""
        self.hana_conn = None
        self.queries = None
    
    def transfer_electric_fact_data(self):
        """
        Ejecuta la transferencia completa de datos de facturación eléctrica.
        
        Returns:
            dict: Resultado de la operación con status, message y detalles.
        """
        result = {
            'status': 'error',
            'message': '',
            'details': {
                'records_processed': 0,
                'temp_table_cleaned': False,
                'steps_completed': []
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
            self.queries = ElectricFactQueries(self.hana_conn)
            
            # 1. Obtener datos de la tabla temporal
            result['details']['steps_completed'].append("Iniciando proceso de transferencia")
            
            temp_result = self.queries.get_temp_electric_fact_data()
            if not temp_result:
                result['message'] = "Error: No se pudieron obtener los datos de la tabla temporal."
                return result

            temp_columns, temp_data = temp_result
                        
            # Validar si la tabla temporal está vacía
            if not temp_data or len(temp_data) == 0:
                result['status'] = 'success'
                result['message'] = "Proceso completado: La tabla temporal está vacía, no hay datos para transferir."
                result['details']['records_processed'] = 0
                result['details']['steps_completed'].append("Tabla temporal vacía - No hay datos para procesar")
                result['details']['temp_table_cleaned'] = True
                
                # Limpiar tabla temporal aunque esté vacía (por consistencia)
                try:
                    self.queries.truncate_temp_table()
                    result['details']['steps_completed'].append("Tabla temporal limpiada")
                except Exception as e:
                    result['details']['steps_completed'].append(f"Advertencia: Error al limpiar tabla temporal: {str(e)}")
                
                return result
            
            result['details']['records_processed'] = len(temp_data)
            result['details']['steps_completed'].append(f"Obtenidos {len(temp_data)} registros de tabla temporal")
            
            # 2. Obtener estructura de la tabla destino
            target_columns = self.queries.get_electric_fact_columns()
            if not target_columns:
                result['message'] = "Error: No se pudieron obtener las columnas de la tabla destino."
                return result
            
            result['details']['steps_completed'].append("Obtenida estructura de tabla destino")
            
            # 3. Comparar estructuras de las tablas
            comparison = self.queries.compare_table_columns(temp_columns, target_columns)
            result['details']['steps_completed'].append("Comparadas estructuras de tablas")
            
            # 4. Verificar si se puede proceder con la inserción
            if not comparison['columns_match']:
                result['message'] = "Error: Las estructuras de las tablas no coinciden completamente."
                result['details']['comparison'] = comparison
                return result
            
            result['details']['steps_completed'].append("Estructuras de tablas compatibles")
            
            # 5. Transferir datos
            success = self.queries.insert_electric_fact_data(temp_data, temp_columns, target_columns)
            if not success:
                result['message'] = "Error durante la transferencia de datos."
                return result
            
            result['details']['steps_completed'].append("Datos transferidos exitosamente")
            
            # 6. Truncar tabla temporal
            truncate_success = self.queries.truncate_temp_table()
            print("Borrando tabla...")
            result['details']['temp_table_cleaned'] = truncate_success
            
            if truncate_success:
                result['details']['steps_completed'].append("Tabla temporal limpiada")
                result['status'] = 'success'
                result['message'] = "Proceso completado exitosamente. Tabla temporal limpiada."
            else:
                result['status'] = 'partial_success'
                result['message'] = "Transferencia exitosa, pero error al limpiar tabla temporal."
            
            return result
            
        except Exception as e:
            result['message'] = f"Error inesperado: {str(e)}"
            result['details']['steps_completed'].append(f"Error inesperado: {str(e)}")
            return result
        
        finally:
            # Cerrar conexión
            if self.hana_conn:
                self.hana_conn.close()
    
    def get_temp_data_preview(self, limit=5, format_type='objects'):
        """
        Obtiene una vista previa de los datos en la tabla temporal.
        
        Args:
            limit (int): Número máximo de registros a retornar.
            format_type (str): 'objects' para objetos con propiedades, 'arrays' para arreglos.
            
        Returns:
            dict: Resultado con preview de datos o error.
        """
        result = {
            'status': 'error',
            'message': '',
            'data': {
                'columns': [],
                'rows': [],
                'total_count': 0
            }
        }
        
        try:
            # Crear instancia de conexión
            self.hana_conn = HanaConnection()
            
            if not self.hana_conn.connect():
                result['message'] = "No se pudo establecer la conexión a la base de datos."
                return result
            
            # Crear instancia de consultas
            self.queries = ElectricFactQueries(self.hana_conn)
            
            # Obtener datos
            temp_result = self.queries.get_temp_electric_fact_data()
            if not temp_result:
                result['message'] = "No se pudieron obtener los datos de la tabla temporal."
                return result
            
            temp_columns, temp_data = temp_result
            
            # Validar si la tabla temporal está vacía
            if not temp_data or len(temp_data) == 0:
                result['status'] = 'success'
                result['message'] = 'La tabla temporal está vacía - No hay datos para mostrar'
                result['data']['total_count'] = 0
                
                if format_type == 'objects':
                    result['data']['records'] = []
                    # No incluir columns y rows en formato objects
                    del result['data']['columns']
                    del result['data']['rows']
                else:
                    # Formato de arreglos (formato original)
                    result['data']['columns'] = temp_columns if temp_columns else []
                    result['data']['rows'] = []
                
                return result
            
            limited_data = temp_data[:limit] if temp_data else []
            
            result['status'] = 'success'
            result['message'] = 'Preview obtenido exitosamente'
            result['data']['total_count'] = len(temp_data)
            
            if format_type == 'objects':
                # Formato de objetos con propiedades nombradas
                result['data']['records'] = []
                for row in limited_data:
                    record = {}
                    for i, column in enumerate(temp_columns):
                        record[column] = row[i] if i < len(row) else None
                    result['data']['records'].append(record)
                # No incluir columns y rows en formato objects
                del result['data']['columns']
                del result['data']['rows']
            else:
                # Formato de arreglos (formato original)
                result['data']['columns'] = temp_columns
                result['data']['rows'] = limited_data
            
            return result
            
        except Exception as e:
            result['message'] = f"Error inesperado: {str(e)}"
            return result
        
        finally:
            if self.hana_conn:
                self.hana_conn.close()