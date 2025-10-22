from utils.db_connection import HanaConnection
from queries.TLCL02_queries import TLCL02Queries

class TLCL02Service:
    """Servicio para gestionar la transferencia de datos de KPI."""
    
    def __init__(self):
        """Inicializa el servicio."""
        self.hana_conn = None
        self.queries = None

    def transfer_kpi_data(self):
        """
        Ejecuta la transferencia completa de datos de KPI.
        
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
            self.queries = TLCL02Queries(self.hana_conn)

            # 1. Obtener datos de la tabla temporal
            result['details']['steps_completed'].append("Iniciando proceso de transferencia")
            
            temp_result = self.queries.get_temp_kpi_data()
            if not temp_result:
                result['message'] = "Error: No se pudieron obtener los datos de la tabla temporal."
                return result

            temp_columns, temp_data = temp_result
                        
            # print('Columnas fuente: ', temp_columns)
            # print('Filas: ', temp_data)
            # print('Result: ', temp_result)

            # Validar si la tabla temporal está vacía
            if not temp_data or len(temp_data) == 0:
                result['status'] = 'success'
                result['message'] = "La tabla temporal está vacía, no hay datos para transferir."
                result['details']['records_processed'] = 0
                result['details']['steps_completed'].append("Tabla temporal vacía - No hay datos para procesar")
                result['details']['temp_table_cleaned'] = True

                # Limpiar tabla temporal aunque esté vacía (por consistencia)
                try:
                    print('Aqui se limpia la tabla TEMP por cuestiones de integridad de datos')
                    # self.queries.truncate_temp_kpi_table()
                    result['details']['temp_table_cleaned'] = True
                except Exception as e:
                    result['details']['temp_table_cleaned'] = False

                return result

            result['details']['records_processed'] = len(temp_data)
            result['details']['steps_completed'].append(f"Registros obtenidos de tabla temporal: {len(temp_data)}")

            # 2. Obtener estructura de la tabla destino
            target_columns = self.queries.get_kpi_table_columns()
            # print('Columnas destino: ', target_columns)
            # print('Hola1')
            if not target_columns:
                result['message'] = "Error: No se pudieron obtener las columnas de la tabla destino."
                return result
            
            result['details']['steps_completed'].append("Estructura destino obtenida")
            
            # 3. Comparar estructuras de las tablas
            comparison = self.queries.compare_table_columns(temp_columns, target_columns)
            result['details']['steps_completed'].append("Comparadas estructuras de tablas")

            # 4. Verificar si se puede proceder con la inserción
            if not comparison['columns_match']:
                result['message'] = "Error: Las columnas de la tabla temporal no coinciden con la tabla destino."
                return result
            
            result['details']['steps_completed'].append("Estructuras de tablas compatibles")

            # 5. Formatear datos con campos calculados en posiciones específicas
            formatted_data = []
            for row in temp_data:
                # Crear una copia de la fila original
                formatted_row = list(row)
                
                # Buscar el campo FECHA (asumiendo que está en alguna posición)
                fecha_value = None
                fecha_index = None
                
                # Buscar FECHA en las columnas temporales
                for i, col_name in enumerate(temp_columns):
                    if col_name.upper() == 'FECHA':
                        fecha_value = row[i]
                        fecha_index = i
                        break
                
                if fecha_value and fecha_index is not None:
                    # Calcular campos de fecha usando la función existente
                    date_fields = self.queries.calculate_date_fields(fecha_value)
                    
                    if date_fields:
                        # Insertar ANIO, MES, DIA en posiciones 3, 4, 5 (índices 2, 3, 4)
                        # Insertar en orden correcto: ANIO primero, luego MES, luego DIA
                        formatted_row.insert(2, date_fields['ANIO'])   # Posición 3
                        formatted_row.insert(3, date_fields['MES'])    # Posición 4  
                        formatted_row.insert(4, date_fields['DIA'])    # Posición 5
                        
                        # Agregar MESANIO al final
                        formatted_row.append(date_fields['MESANIO'])
                        
                        # print(f"Fila formateada: {formatted_row}")
                        # print(f"ANIO: {date_fields['ANIO']} (pos 3), MES: {date_fields['MES']} (pos 4), DIA: {date_fields['DIA']} (pos 5), MESANIO: {date_fields['MESANIO']} (final)")
                
                formatted_data.append(formatted_row)
            
            # Actualizar temp_columns para incluir los campos calculados
            # Insertar ANIO, MES, DIA después de HORA (posición 2)
            updated_temp_columns = temp_columns.copy()
            updated_temp_columns.insert(2, 'ANIO')   # Posición 3
            updated_temp_columns.insert(3, 'MES')    # Posición 4
            updated_temp_columns.insert(4, 'DIA')    # Posición 5
            # Agregar MESANIO al final
            updated_temp_columns.append('MESANIO')
            
            # print('Columnas actualizadas: ', updated_temp_columns)
            
            # print('Datos originales: ', temp_data[0] if temp_data else 'No hay datos')
            # print('Datos formateados: ', formatted_data[0] if formatted_data else 'No hay datos formateados')

            # print('-----------------')

            # 5. Transferir datos formateados (no los originales)
            success = self.queries.insert_kpi_data(formatted_data, updated_temp_columns, target_columns)
            if not success:
                result['message'] = "Error durante la transferencia de datos."
                return result

            result['details']['steps_completed'].append(f"Datos transferidos exitosamente: {len(formatted_data)} registros")
            result['status'] = 'success'
            result['message'] = f"Transferencia completada exitosamente. {len(formatted_data)} registros procesados."

            # 6. Truncar tabla temporal
            # truncate_success = self.queries.truncate_temp_table()
            # print("Borrando tabla...")
            # result['details']['temp_table_truncated'] = truncate_success

            # if truncate_success:
            #     result['details']['steps_completed'].append("Tabla temporal truncada exitosamente")
            # else:
            #     result['message'] = "Error: No se pudo truncar la tabla temporal."
            #     return result

            return result

        except Exception as e:
            result['message'] = f"Error en la conexión a la base de datos: {str(e)}"
            return result