"""
Módulo para gestionar las consultas SQL específicas del proceso TLCL03_Counters.
Incluye utilidades para ejecutar el script SQL de merges definido en TLCL03_merge.sql.
"""

import os
from utils.sql_runner import SqlRunner
from utils.config import DB_CONFIG

class TLCL03Queries:
    """Clase para gestionar las consultas específicas del proceso TLCL03_Counters."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection
    
    def run_tlcl03_sql_script(self):
        """Ejecuta el script SQL de TLCL03_Counters ubicado en queries/TLCL03_merge.sql.

        Ejecuta las sentencias MERGE para SITE, ADDRESS, GATEWAY y SIM.

        Returns:
            dict: Resumen de ejecución con número de sentencias ejecutadas y estado.
        """

        try: 
            # Construir ruta absoluta del archivo SQL
            base_dir = os.path.dirname(os.path.abspath(__file__))
            sql_path = os.path.join(base_dir, 'TLCL03_merge.sql')

            # Usar el runner común para ejecutar el archivo
            runner = SqlRunner(self.connection)
            res = runner.execute_sql_file(sql_path, commit_mode='end', stop_on_error=True)

            # print('res', res)

            # Ajustar mensaje y detalles para TLCL03_Counters
            if res['success']:
                res['message'] = 'Script TLCL03_Counters_Merge ejecutado correctamente'
            else:
                res['message'] = f"Error al ejecutar script TLCL03_Counters: {res.get('message', 'desconocido')}"
            return res
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al ejecutar script TLCL03_Counters: {str(e)}',
                'details': None
            }

    def get_table_columns(self, table_name):
        try:
            cursor = self.connection.cursor
            query = f"""
            SELECT COLUMN_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE SCHEMA_NAME = '{DB_CONFIG['schema']}' 
            AND TABLE_NAME = '{table_name.split('.')[-1]}'
            ORDER BY POSITION
            """
            print('query temp', query)
            cursor.execute(query)
            columns = [row[0] for row in cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de la tabla {table_name}")
            return None

    def get_temp_huawei_counters_data(self):
        try:
            cursor = self.connection.cursor
            columns = self.get_table_columns('TELCEL_EE_TEMPHUAWEICOUNTERS')
            if not columns:
                return None

            query = f"SELECT * FROM \"{DB_CONFIG['schema']}\".\"TELCEL_EE_TEMPHUAWEICOUNTERS\""
            cursor.execute(query)
            raw_data = cursor.fetchall()

            # print('raw_data', raw_data)

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
            print(f"Error al obtener datos de KPI temporal")
            return None

    def get_huawei_counters_table_columns(self):
        """Obtiene las columnas de la tabla TELCEL_EE_HUAWEICOUNTERS.
        
        Returns:
            list: Lista de nombres de columnas de tabla target.
            None: Si ocurre un error.
        """
        return self.get_table_columns('TELCEL_EE_HUAWEICOUNTERS')

    def compare_table_columns(self, temp_columns, target_columns):
        """Compara las columnas de las tablas temporal y destino.
        Excluye campos calculados que se generan a partir de otros campos.
        
        Args:
            temp_columns (list): Columnas de la tabla temporal.
            target_columns (list): Columnas de la tabla destino.
            
        Returns:
            dict: Diccionario con el resultado de la comparación.
        """
        # Definir campos calculados que se excluyen de la comparación
        calculated_fields = {'MESANIO', 'ANIO', 'MES', 'DIA'}
        
        temp_set = set(temp_columns)
        target_set = set(target_columns)
        
        # Filtrar campos calculados de la tabla destino para la comparación
        target_set_filtered = target_set - calculated_fields
        
        common_columns = temp_set.intersection(target_set_filtered)
        only_in_temp = temp_set - target_set_filtered
        only_in_target = target_set_filtered - temp_set
        
        # Verificar si tiene campo MESANIO en la tabla temporal (no debería tenerlo)
        has_mesanio = 'MESANIO' in temp_set
        
        # Verificar si la tabla destino tiene los campos calculados
        has_calculated_fields = bool(calculated_fields.intersection(target_set))
        
        # Determinar si las columnas coinciden lo suficiente para proceder
        # Solo verificamos que no haya campos extra en temp (la tabla temporal no debe tener campos que no estén en target)
        # Es normal que target tenga campos adicionales (campos calculados) que no estén en temp
        columns_match = len(only_in_temp) == 0
        
        return {
            'common_columns': list(common_columns),
            'only_in_temp': list(only_in_temp),
            'only_in_target': list(only_in_target),
            'calculated_fields_in_target': list(calculated_fields.intersection(target_set)),
            'has_mesanio': has_mesanio,
            'has_calculated_fields': has_calculated_fields,
            'columns_match': columns_match
        }

    def calculate_date_fields(self, fecha):
        """Extrae los campos FECHA, HORA, ANIO, MES, DIA y MESANIO basado en el campo FECHA original.
        
        Args:
            fecha: Valor de fecha en formato YYYY-MM-DD HH:MM:SS o YYYY-MM-DD.
            
        Returns:
            dict: Diccionario con los campos extraídos:
                - FECHA: Fecha en formato YYYY-MM-DD (sin hora)
                - HORA: Hora en formato HH:MM:00 (extraída de FECHA o 00:00:00 por defecto)
                - ANIO: Año (YYYY)
                - MES: Mes (MM)
                - DIA: Día (DD)
                - MESANIO: Mes y año en formato MM.YYYY
            None si hay error en el procesamiento.
        """
        try:
            # Validar que la fecha no esté vacía
            if not fecha or str(fecha).strip() == '':
                print("Error: Campo FECHA está vacío")
                return None
            
            fecha_str = str(fecha).strip()
            
            # Extraer fecha y hora
            fecha_parte = fecha_str
            hora_parte = "00:00:00"  # Valor por defecto
            
            # Manejar formato YYYY-MM-DD HH:MM:SS
            if ' ' in fecha_str:
                partes = fecha_str.split(' ')
                fecha_parte = partes[0]
                if len(partes) > 1:
                    hora_completa = partes[1]
                    # Extraer HH:MM y agregar :00 para los segundos
                    if ':' in hora_completa:
                        hora_partes = hora_completa.split(':')
                        if len(hora_partes) >= 2:
                            hora_parte = f"{hora_partes[0]:0>2}:{hora_partes[1]:0>2}:00"
                        else:
                            hora_parte = f"{hora_partes[0]:0>2}:00:00"
            
            # Dividir la fecha por el separador '-'
            fecha_parts = fecha_parte.split('-')
            
            # Validar que tenga exactamente 3 partes
            if len(fecha_parts) != 3:
                print(f"Error: Formato de fecha inválido. Esperado YYYY-MM-DD, recibido: {fecha}")
                return None
            
            # Extraer año, mes y día
            anio_str, mes_str, dia_str = fecha_parts
            
            # Convertir a enteros para validación
            anio = int(anio_str)
            mes = int(mes_str)
            dia = int(dia_str)
            
            # Validar rangos
            if not (1 <= mes <= 12):
                print(f"Error: Mes inválido ({mes}). Debe estar entre 1 y 12")
                return None
            
            if not (1 <= dia <= 31):
                print(f"Error: Día inválido ({dia}). Debe estar entre 1 y 31")
                return None
            
            if anio < 1900 or anio > 2100:
                print(f"Error: Año inválido ({anio}). Debe estar entre 1900 y 2100")
                return None
            
            # Formatear los campos
            mes_formatted = f"{mes:02d}"
            dia_formatted = f"{dia:02d}"
            anio_formatted = str(anio)
            mesanio = f"{mes:02d}.{anio}"
            fecha_formatted = f"{anio}-{mes_formatted}-{dia_formatted}"
            
            return {
                'FECHA': fecha_formatted,
                'HORA': hora_parte,
                'ANIO': anio_formatted,
                'MES': mes_formatted,
                'DIA': dia_formatted,
                'MESANIO': mesanio
            }
            
        except (ValueError, TypeError, IndexError) as e:
            print(f"Error al procesar fecha '{fecha}': {e}")
            return None

    def insert_huawei_counters_data(self, temp_data, temp_columns, target_columns):
        """Realiza un upsert (insert o update) de los datos de la tabla temporal en la tabla final.
        
        Args:
            temp_data (list): Datos de la tabla temporal.
            temp_columns (list): Columnas de la tabla temporal.
            target_columns (list): Columnas de la tabla destino.
            
        Returns:
            bool: True si el upsert fue exitoso, False en caso contrario.
        """
        try:
            # print('Hola desde el insert')
            cursor = self.connection.cursor
            
            # Obtener columnas comunes (excluyendo campos calculados que se agregan después)
            common_columns = [col for col in temp_columns if col in target_columns and col not in ['FECHA', 'MESANIO', 'ANIO', 'MES', 'DIA', 'HORA']]

            # Agregar campos calculados solo si están en target_columns (sin duplicar)
            calculated_fields = ['FECHA', 'HORA', 'ANIO', 'MES', 'DIA', 'MESANIO']
            for field in calculated_fields:
                if field in target_columns:
                    common_columns.append(field)  

            # Definir las columnas clave (llaves primarias)
            primary_key_columns = ['FECHA', 'HORA', 'ANIO', 'BTSNAME', 'IDBTSNAME', 'MESANIO']

            # Filtrar solo las columnas clave que existen en common_columns
            existing_key_columns = [col for col in primary_key_columns if col in common_columns]

            # Columnas para actualizar (todas excepto las llaves primarias)
            update_columns = [col for col in common_columns if col not in primary_key_columns]

            # print(f"Columnas comunes: {common_columns}")
            # print(f"Columnas clave existentes: {existing_key_columns}")
            # print(f"Columnas para actualizar: {update_columns}")

            # Construir la consulta UPSERT usando sintaxis correcta de SAP HANA
            table_name = f'"{DB_CONFIG["schema"]}"."TELCEL_EE_HUAWEICOUNTERS"'
            
            # Ordenar campos correctamente: FECHA, HORA, ANIO, MES, DIA, resto_de_campos, MESANIO
            ordered_columns = []
            
            # 1. Agregar campos principales en orden específico
            priority_fields = ['FECHA', 'HORA', 'ANIO', 'MES', 'DIA']
            for field in priority_fields:
                if field in common_columns:
                    ordered_columns.append(field)
            
            # 2. Agregar resto de campos (excepto MESANIO)
            for col in common_columns:
                if col not in priority_fields and col != 'MESANIO':
                    ordered_columns.append(col)
            
            # 3. Agregar MESANIO al final si existe
            if 'MESANIO' in common_columns:
                ordered_columns.append('MESANIO')
            
            # Crear placeholders y columnas ordenadas
            placeholders = ', '.join(['?' for _ in ordered_columns])
            columns_str = ', '.join([f'"{col}"' for col in ordered_columns])
            
            # Condiciones WHERE para las claves primarias
            where_conditions = ' AND '.join([f'"{col}" = ?' for col in existing_key_columns])
            
            # Usar UPSERT con WHERE (sintaxis que funciona)
            upsert_query = f"""
            UPSERT {table_name} ({columns_str})
            VALUES ({placeholders})
            WHERE {where_conditions}
            """

            print(f"Consulta UPSERT: {upsert_query}")

            # Procesar cada fila de datos
            records_processed = 0
            records_failed = 0

            print(f"Filas a procesar: {len(temp_data)}")
            
            for row in temp_data:
                try:
                    # Preparar los valores para la inserción según ordered_columns
                    # Los datos ya vienen formateados desde formatted_data, no necesitamos recalcular
                    values_to_insert = []
                    
                    for col in ordered_columns:
                        if col in temp_columns:
                            # Usar valor directamente de la fila (ya viene con campos calculados)
                            col_index = temp_columns.index(col)
                            values_to_insert.append(row[col_index])
                        else:
                            # Valor por defecto si no se encuentra
                            values_to_insert.append(None)
                    
                    # Ejecutar UPSERT con parámetros dobles
                    # Para UPSERT necesitamos los datos dos veces: una para VALUES y otra para WHERE
                    upsert_params = list(values_to_insert)  # Para VALUES
                    where_params = [values_to_insert[ordered_columns.index(col)] for col in existing_key_columns if col in ordered_columns]  # Para WHERE
                    all_params = upsert_params + where_params
                    
                    # DEBUG: Mostrar información detallada del primer registro
                    # if records_processed == 0:
                    #     print(f"=== DEBUG PRIMER REGISTRO ===")
                    #     print(f"ordered_columns: {ordered_columns}")
                    #     print(f"values_to_insert: {values_to_insert}")
                    #     print(f"existing_key_columns: {existing_key_columns}")
                    #     print(f"upsert_params: {upsert_params}")
                    #     print(f"where_params: {where_params}")
                    #     print(f"all_params: {all_params}")
                    #     print(f"Query: {upsert_query}")
                    #     print("=" * 50)
                    
                    cursor.execute(upsert_query, all_params)
                    records_processed += 1
                    
                    if records_processed % 100 == 0:
                        print(f"Procesados {records_processed} registros...")
                        
                except Exception as row_error:
                    print(f"Error al procesar fila {records_processed + records_failed + 1}: {row_error}")
                    records_failed += 1
                    continue
            
            # Confirmar transacción
            self.connection.connection.commit()
            
            print(f"Inserción completada: {records_processed} registros procesados, {records_failed} fallidos")
            return True

        except Exception as e:
            print(f"Error al insertar datos en la tabla final: {e}")
            # Rollback en caso de error
            try:
                self.connection.connection.rollback()
            except:
                pass
            return False

    def truncate_temp_huawei_counters_table(self):
        """Trunca la tabla temporal TELCEL_EE_TEMPHUAWEICOUNTERS.
        
        Returns:
            bool: True si el truncate fue exitoso, False en caso contrario.
        """
        try:
            cursor = self.connection.cursor
            query = f"TRUNCATE TABLE \"{DB_CONFIG['schema']}\".\"TELCEL_EE_TEMPHUAWEICOUNTERS\""
            cursor.execute(query)
            self.connection.connection.commit()  # Usar connection.connection.commit()
            return True
        except Exception as e:
            print(f"Error al truncar tabla temporal: {e}")
            return False





