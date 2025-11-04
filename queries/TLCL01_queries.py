from sqlite3 import Cursor
from utils.config import DB_CONFIG
try:
    # Importar hdbcli si está disponible para soportar OUT parameters vía callproc
    from hdbcli import dbapi as hana_dbapi
except Exception:
    hana_dbapi = None

class TLCL01Queries:
    """
    Clase para gestionar las consultas del proceso TLCL01 - Electric Fact.
    Replica el graph de SAP Data Intelligence para transferir datos de 
    TELCEL_EE_TEMPELECTRICFACT a TELCEL_EE_ELECTRICFACT con transformaciones.
    """

    def __init__(self, connection):
        self.connection = connection

    def get_table_columns(self, table_name):
        """Obtiene las columnas de una tabla específica."""
        try:
            cursor = self.connection.cursor
            query = f"""
            SELECT COLUMN_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE SCHEMA_NAME = '{DB_CONFIG['schema']}' 
            AND TABLE_NAME = '{table_name.split('.')[-1]}'
            ORDER BY POSITION
            """
            cursor.execute(query)
            columns = [row[0] for row in cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de la tabla {table_name}: {e}")
            return None

    def get_temp_electric_fact_data(self):
        """
        Obtiene datos de la tabla temporal TELCEL_EE_TEMPELECTRICFACT.
        Equivalente al Table Consumer (tableconsumer1) del graph SAP DI.
        """
        try:
            cursor = self.connection.cursor
            columns = self.get_table_columns('TELCEL_EE_TEMPELECTRICFACT')
            if not columns:
                return None

            query = f'SELECT * FROM "{DB_CONFIG["schema"]}"."TELCEL_EE_TEMPELECTRICFACT"'
            cursor.execute(query)
            raw_data = cursor.fetchall()

            data = []
            for row in raw_data:
                # Convertir cada valor a tipo básico de Python
                converted_row = []
                for value in row:
                    if value is None:
                        converted_row.append(None)
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        converted_row.append(value.isoformat())
                    else:
                        converted_row.append(str(value) if value is not None else None)
                data.append(converted_row)

            return columns, data
        except Exception as e:
            print(f"Error al obtener datos de TELCEL_EE_TEMPELECTRICFACT: {e}")
            return None

    def transform_data_with_mesanio(self, columns, data):
        """
        Aplica la transformación de datos agregando el campo MESANIO.
        Equivalente al Data Transform (datatransform1) del graph SAP DI.
        
        MESANIO se calcula como: MESFACENC (2 dígitos) + "." + ANIOFACENC
        Ejemplo: Si MESFACENC=3 y ANIOFACENC=2024, entonces MESANIO="03.2024"
        """
        try:
            # Encontrar índices de las columnas necesarias
            mesfacenc_idx = columns.index('MESFACENC') if 'MESFACENC' in columns else None
            aniofacenc_idx = columns.index('ANIOFACENC') if 'ANIOFACENC' in columns else None
            
            if mesfacenc_idx is None or aniofacenc_idx is None:
                print("Error: No se encontraron las columnas MESFACENC o ANIOFACENC")
                return None

            # Agregar MESANIO a las columnas
            transformed_columns = columns + ['MESANIO']
            transformed_data = []

            for row in data:
                # Crear MESANIO: formato MM.YYYY
                mesfacenc = row[mesfacenc_idx]
                aniofacenc = row[aniofacenc_idx]
                
                if mesfacenc is not None and aniofacenc is not None:
                    # Asegurar que el mes tenga 2 dígitos
                    mes_formatted = str(mesfacenc).zfill(2)
                    mesanio = f"{mes_formatted}.{aniofacenc}"
                else:
                    mesanio = None

                # Agregar MESANIO al final de la fila
                transformed_row = list(row) + [mesanio]
                transformed_data.append(transformed_row)

            return transformed_columns, transformed_data
        except Exception as e:
            print(f"Error en la transformación de datos: {e}")
            return None

    def get_electric_fact_table_columns(self):
        """Obtiene las columnas de la tabla destino TELCEL_EE_ELECTRICFACT."""
        return self.get_table_columns('TELCEL_EE_ELECTRICFACT')

    def upsert_electric_fact_data(self, columns, data):
        """
        Realiza INSERT en la tabla TELCEL_EE_ELECTRICFACT.
        Equivalente al Table Producer (tableproducer1) del graph SAP DI.
        
        NOTA: Cambiado de UPSERT a INSERT para permitir múltiples registros
        con el mismo MESANIO pero diferentes CLRPU.
        """
        try:
            cursor = self.connection.cursor
            
            # Obtener columnas de la tabla destino
            target_columns = self.get_electric_fact_table_columns()
            if not target_columns:
                return False

            # Filtrar solo las columnas que existen en la tabla destino
            valid_columns = []
            valid_indices = []
            
            for i, col in enumerate(columns):
                if col in target_columns:
                    valid_columns.append(col)
                    valid_indices.append(i)

            if not valid_columns:
                print("Error: No hay columnas válidas para insertar")
                return False

            # Construir la consulta UPSERT
            columns_str = ', '.join([f'"{col}"' for col in valid_columns])
            placeholders = ', '.join(['?' for _ in valid_columns])
            
            # Crear la cláusula de actualización para UPSERT
            update_clause = ', '.join([f'"{col}" = VALUES("{col}")' for col in valid_columns])
            
            # Cambiar UPSERT por INSERT para permitir múltiples registros
            # con el mismo MESANIO pero diferentes CLRPU
            insert_query = f"""
            INSERT INTO "{DB_CONFIG['schema']}"."TELCEL_EE_ELECTRICFACT" 
            ({columns_str}) 
            VALUES ({placeholders})
            """

            # Preparar los datos para inserción
            insert_data = []
            for row in data:
                filtered_row = [row[i] for i in valid_indices]
                insert_data.append(filtered_row)

            # Ejecutar UPSERT por lotes
            batch_size = 1000
            total_processed = 0
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                total_processed += len(batch)
                
                # Commit cada lote
                self.connection.connection.commit()

            print(f"INSERT completado: {total_processed} registros procesados")
            return True

        except Exception as e:
            print(f"Error en INSERT de TELCEL_EE_ELECTRICFACT: {e}")
            self.connection.connection.rollback()
            return False

    def truncate_temp_electric_fact_table(self):
        """
        Trunca la tabla temporal TELCEL_EE_TEMPELECTRICFACT.
        Equivalente al SQL Executor (flowagentsqlexecutor1) del graph SAP DI.
        """
        try:
            cursor = self.connection.cursor
            truncate_query = f'TRUNCATE TABLE "{DB_CONFIG["schema"]}"."TELCEL_EE_TEMPELECTRICFACT"'
            cursor.execute(truncate_query)
            self.connection.connection.commit()
            print("Tabla temporal TELCEL_EE_TEMPELECTRICFACT truncada exitosamente")
            return True
        except Exception as e:
            print(f"Error al truncar tabla temporal: {e}")
            self.connection.connection.rollback()
            return False

    def get_electric_fact_count(self):
        """Obtiene el conteo de registros en la tabla TELCEL_EE_ELECTRICFACT."""
        try:
            cursor = self.connection.cursor
            query = f'SELECT COUNT(*) FROM "{DB_CONFIG["schema"]}"."TELCEL_EE_ELECTRICFACT"'
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error al obtener conteo de TELCEL_EE_ELECTRICFACT: {e}")
            return None

    def get_temp_electric_fact_count(self):
        """Obtiene el conteo de registros en la tabla temporal."""
        try:
            cursor = self.connection.cursor
            query = f'SELECT COUNT(*) FROM "{DB_CONFIG["schema"]}"."TELCEL_EE_TEMPELECTRICFACT"'
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error al obtener conteo de TELCEL_EE_TEMPELECTRICFACT: {e}")
            return None

    def execute_SP_TLCL_01_sp(self,  param1=0, param2=''):
        """
        Ejecuta el stored procedure SP_TLCL_01 que encapsula todo el proceso TLCL01.

        Returns:
            dict: Resultado de la ejecución del stored procedure.
        """
        try:
            cursor = self.connection.cursor
            schema = DB_CONFIG['schema']

            # 1) Intento 1: llamada directa tipo CALL con parámetros de entrada
            cursor.execute(f"CALL {schema}.SP_TLCL_01(?, ?)", (param1, param2))

            # Intentar obtener resultado si el SP devuelve algo
            try:
                result = cursor.fetchone()
                if result:
                    flag = result[0] if len(result) > 0 else None
                    mensaje_error = result[1] if len(result) > 1 else None

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
                    # Sin result set: intentamos capturar OUT params (SUCCESS_FLAG, MESSAGE)
                    raise Exception('No result set')
            except Exception as e:
                # Caso común: el SP no devuelve result set, intentar OUT params con callproc
                if hana_dbapi is not None:
                    try:
                        # 2) Intento 2: usar callproc con IN params + OUT params
                        # En hdbcli, los OUT params se pasan como None y el driver los rellena
                        proc_params = [param1, param2, None, None]
                        proc_result = cursor.callproc(f"{schema}.SP_TLCL_01", proc_params)

                        # Algunas implementaciones devuelven None y actualizan proc_params
                        if proc_result is None:
                            proc_result = proc_params

                        # Los dos últimos elementos deberían ser los OUT params
                        flag = proc_result[-2] if len(proc_result) >= 2 else None
                        mensaje_error = proc_result[-1] if len(proc_result) >= 2 else None

                        success = flag == 1 if flag is not None else True

                        return {
                            'success': success,
                            'message': 'Stored procedure ejecutado correctamente',
                            'data': {
                                'flag': flag,
                                'mensaje_error': mensaje_error
                            }
                        }
                    except Exception as ce:
                        # No se pudieron capturar OUT params
                        return {
                            'success': True,
                            'message': 'Stored procedure ejecutado correctamente (sin datos del SP)',
                            'data': {
                                'flag': None,
                                'mensaje_error': str(ce)
                            }
                        }
                else:
                    # Cliente HANA no disponible; no se puede capturar OUT params
                    return {
                        'success': True,
                        'message': 'Stored procedure ejecutado correctamente',
                        'data': {
                            'flag': None,
                            'mensaje_error': 'SP ejecutado sin devolver datos (instalar hdbcli para OUT params)'
                        }
                    }

        except Exception as e:
            return {
                'success': False,
                'message': f"Error al ejecutar stored procedure: {str(e)}",
                'data': None
            }