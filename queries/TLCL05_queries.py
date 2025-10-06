"""
Módulo para gestionar las consultas SQL específicas de facturación eléctrica.
Contiene todas las queries relacionadas con TELCEL_EE_ELECTRICFACT y TELCEL_EE_TEMPELECTRICFACT.
"""

from config import DB_CONFIG

class TLCL05Queries:
    """Clase para gestionar las consultas específicas de facturación eléctrica."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection
    
    def get_table_columns(self, table_name):
        """Obtiene los nombres de las columnas de una tabla.
        
        Args:
            table_name (str): Nombre completo de la tabla (esquema.tabla).
            
        Returns:
            list: Lista de nombres de columnas.
            None: Si ocurre un error.
        """
        try:
            cursor = self.connection.cursor
            # Consulta para obtener metadatos de columnas usando SYS.TABLE_COLUMNS
            query = f"""
            SELECT COLUMN_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE SCHEMA_NAME = '{DB_CONFIG['schema']}' 
            AND TABLE_NAME = '{table_name.split('.')[-1]}'
            ORDER BY POSITION
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return [row[0] for row in results]
        except Exception as e:
            print(f"Error al obtener columnas de la tabla {table_name}: {e}")
            return None
    
    def get_temp_electric_fact_data(self):
        """Obtiene todos los datos de la tabla temporal TELCEL_EE_TEMPELECTRICFACT.
        
        Returns:
            tuple: (columnas, datos) donde columnas es una lista de nombres de columnas
                   y datos es una lista de listas con los registros.
            None: Si ocurre un error.
        """
        try:
            cursor = self.connection.cursor
            # Primero obtener las columnas
            columns = self.get_table_columns('TELCEL_EE_TEMPELECTRICFACT')
            if not columns:
                return None
            
            # Luego obtener los datos
            query = f"SELECT * FROM \"{DB_CONFIG['schema']}\".\"TELCEL_EE_TEMPELECTRICFACT\""
            cursor.execute(query)
            raw_data = cursor.fetchall()
            
            # Convertir ResultRow a listas para que sea JSON serializable
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
            print(f"Error al obtener datos de la tabla temporal: {e}")
            return None
    
    def get_electric_fact_columns(self):
        """Obtiene las columnas de la tabla TELCEL_EE_ELECTRICFACT.
        
        Returns:
            list: Lista de nombres de columnas.
            None: Si ocurre un error.
        """
        return self.get_table_columns('TELCEL_EE_ELECTRICFACT')
    
    def compare_table_columns(self, temp_columns, target_columns):
        """Compara las columnas de las tablas temporal y destino.
        
        Args:
            temp_columns (list): Columnas de la tabla temporal.
            target_columns (list): Columnas de la tabla destino.
            
        Returns:
            dict: Diccionario con el resultado de la comparación.
        """
        temp_set = set(temp_columns)
        target_set = set(target_columns)
        
        common_columns = temp_set.intersection(target_set)
        only_in_temp = temp_set - target_set
        only_in_target = target_set - temp_set
        
        # Verificar si tiene campo MESANIO
        has_mesanio = 'MESANIO' in temp_set
        
        # Determinar si las columnas coinciden lo suficiente para proceder
        columns_match = len(only_in_temp) == 0 and len(only_in_target) <= 1
        
        return {
            'common_columns': list(common_columns),
            'only_in_temp': list(only_in_temp),
            'only_in_target': list(only_in_target),
            'has_mesanio': has_mesanio,
            'columns_match': columns_match
        }
    
    def calculate_mesanio(self, mesfacenc, aniofacenc):
        """Calcula el campo MESANIO basado en MESFACENC y ANIOFACENC.
        
        Args:
            mesfacenc: Valor del mes de facturación.
            aniofacenc: Valor del año de facturación.
            
        Returns:
            str: Valor calculado de MESANIO en formato MM.YYYY.
        """
        try:
            # Convertir a enteros y formatear
            mes = int(mesfacenc)
            anio = int(aniofacenc)
            
            # Formatear mes con ceros a la izquierda y concatenar con año
            mesanio = f"{mes:02d}.{anio}"
            return mesanio
        except (ValueError, TypeError) as e:
            print(f"Error al calcular MESANIO: {e}")
            return None

    def insert_electric_fact_data(self, temp_data, temp_columns, target_columns):
        """Realiza un upsert (insert o update) de los datos de la tabla temporal en la tabla final.
        
        Args:
            temp_data (list): Datos de la tabla temporal.
            temp_columns (list): Columnas de la tabla temporal.
            target_columns (list): Columnas de la tabla destino.
            
        Returns:
            bool: True si el upsert fue exitoso, False en caso contrario.
        """
        try:
            cursor = self.connection.cursor
            
            # Obtener columnas comunes (excluyendo MESANIO)
            common_columns = [col for col in temp_columns if col in target_columns and col != 'MESANIO']
            
            # Agregar MESANIO si está en las columnas destino
            if 'MESANIO' in target_columns:
                common_columns.append('MESANIO')
            
            # Definir las columnas clave para el MERGE
            key_columns = ['CLRPU', 'TIPOMOV', 'CUENTA', 'CLTARIFA', 'ANIODESDE', 'MESDESDE', 'DIADESDE', 
                          'ANIOHASTA', 'MESHASTA', 'DIAHASTA', 'ANIOFAC', 'MESFAC', 'ANIOFACENC', 
                          'MESFACENC', 'IDPROVEEDOR', 'TIPOARCHIVO', 'MESANIO']
            
            # Filtrar solo las columnas clave que existen en common_columns
            existing_key_columns = [col for col in key_columns if col in common_columns]
            
            # Columnas para actualizar (todas excepto las claves)
            update_columns = [col for col in common_columns if col not in existing_key_columns]
            
            # Preparar datos para upsert
            upsert_data = []
            for row in temp_data:
                row_data = list(row)
                
                # Calcular MESANIO si es necesario
                if 'MESANIO' in target_columns:
                    # Buscar MESFACENC y ANIOFACENC en los datos
                    mesfacenc_idx = temp_columns.index('MESFACENC') if 'MESFACENC' in temp_columns else None
                    aniofacenc_idx = temp_columns.index('ANIOFACENC') if 'ANIOFACENC' in temp_columns else None
                    
                    if mesfacenc_idx is not None and aniofacenc_idx is not None:
                        mesanio = self.calculate_mesanio(row[mesfacenc_idx], row[aniofacenc_idx])
                        row_data.append(mesanio)
                    else:
                        print("Advertencia: No se encontraron MESFACENC o ANIOFACENC para calcular MESANIO")
                        row_data.append(None)
                
                upsert_data.append(tuple(row_data))
            
            # Crear consulta UPSERT (más simple que MERGE)
            placeholders = ', '.join(['?' for _ in common_columns])
            
            # Condiciones WHERE para las claves primarias
            where_conditions = ' AND '.join([f'"{col}" = ?' for col in existing_key_columns])
            
            # Crear la consulta UPSERT
            query = f"""
            UPSERT \"{DB_CONFIG['schema']}\".\"TELCEL_EE_ELECTRICFACT\" 
            ({', '.join([f'"{col}"' for col in common_columns])}) 
            VALUES ({placeholders}) 
            WHERE {where_conditions}
            """
            
            # Ejecutar upsert por lotes
            inserted_count = 0  # Inicializar contador
            
            for data_row in upsert_data:
                # Para UPSERT necesitamos los datos dos veces: una para VALUES y otra para WHERE
                upsert_params = list(data_row)  # Para VALUES
                where_params = [data_row[common_columns.index(col)] for col in existing_key_columns if col in common_columns]  # Para WHERE
                all_params = upsert_params + where_params
                
                cursor.execute(query, all_params)
                # SAP HANA no proporciona información directa sobre si fue INSERT o UPDATE
                # pero podemos contar las filas afectadas
                inserted_count += 1
            
            self.connection.connection.commit()
            
            print(f"✅ Se procesaron {len(upsert_data)} registros exitosamente (upsert).")
            return True
            
        except Exception as e:
            print(f"Error durante el upsert: {e}")
            # Usar la conexión interna para rollback
            if hasattr(self.connection, 'connection') and self.connection.connection:
                self.connection.connection.rollback()
            return False
    
    def truncate_temp_table(self):
        """Trunca la tabla temporal TELCEL_EE_TEMPELECTRICFACT.
        
        Returns:
            bool: True si el truncate fue exitoso, False en caso contrario.
        """
        try:
            cursor = self.connection.cursor
            query = f"TRUNCATE TABLE \"{DB_CONFIG['schema']}\".\"TELCEL_EE_TEMPELECTRICFACT\""
            cursor.execute(query)
            self.connection.connection.commit()  # Usar connection.connection.commit()
            return True
        except Exception as e:
            print(f"Error al truncar tabla temporal: {e}")
            return False
