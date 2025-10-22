"""
Módulo para gestionar las consultas SQL específicas del proceso TLCL04.
Incluye utilidades para ejecutar el script SQL inicial y gestionar el Table Consumer.
"""

import os
from utils.sql_runner import SqlRunner
from utils.config import DB_CONFIG

class TLCL04Queries:
    """Clase para gestionar las consultas específicas del proceso TLCL04."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection

    def run_tlcl04_initial_sql(self):
        """Ejecuta el script SQL inicial de TLCL04 ubicado en queries/TLCL04_initial.sql.

        Ejecuta las sentencias UPSERT, SELECT INTO y MERGE para procesar datos de Ericsson Counters.

        Returns:
            dict: Resumen de ejecución con número de sentencias ejecutadas y estado.
        """
        try:
            # Construir ruta absoluta del archivo SQL
            base_dir = os.path.dirname(os.path.abspath(__file__))
            sql_path = os.path.join(base_dir, 'TLCL04_initial.sql')

            # Usar el runner común para ejecutar el archivo
            runner = SqlRunner(self.connection)
            res = runner.execute_sql_file(sql_path, commit_mode='end', stop_on_error=True)

            # Ajustar mensaje y detalles para TLCL04
            if res['success']:
                res['message'] = 'Script inicial TLCL04 ejecutado correctamente'
            else:
                res['message'] = f"Error al ejecutar script inicial TLCL04: {res.get('message', 'desconocido')}"
            return res
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al ejecutar script inicial TLCL04: {str(e)}',
                'details': None
            }

    def get_table_columns(self, table_name):
        """Obtiene las columnas de una tabla específica.
        
        Args:
            table_name (str): Nombre de la tabla.
            
        Returns:
            list: Lista de nombres de columnas.
        """
        try:
            query = f"""
            SELECT COLUMN_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE SCHEMA_NAME = '{DB_CONFIG['schema']}' 
            AND TABLE_NAME = '{table_name}'
            ORDER BY POSITION
            """
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de {table_name}: {str(e)}")
            return []

    def get_temp_ericsson_counters_data(self, limit=None):
        """Obtiene datos de la tabla TELCEL_EE_TEMPERICSSONCOUNTERS.
        
        Args:
            limit (int, optional): Límite de registros a obtener.
            
        Returns:
            list: Lista de registros.
        """
        try:
            query = f"""
            SELECT * FROM {DB_CONFIG['schema']}.TELCEL_EE_TEMPERICSSONCOUNTERS
            """
            if limit:
                query += f" LIMIT {limit}"
                
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error al obtener datos de TEMPERICSSONCOUNTERS: {str(e)}")
            return []

    def transform_and_add_date_fields(self, data):
        """Transforma los datos agregando campos de fecha calculados.
        
        Args:
            data (list): Lista de registros de la tabla temporal.
            
        Returns:
            list: Lista de registros transformados con campos adicionales.
        """
        try:
            transformed_data = []
            for row in data:
                # Crear una copia del registro como lista para poder modificarlo
                new_row = list(row)
                
                # Extraer fecha del campo FECHA (asumiendo formato YYYY-MM-DD)
                if len(new_row) > 0 and new_row[0]:  # FECHA está en la primera posición
                    fecha_str = str(new_row[0])
                    if '-' in fecha_str:
                        parts = fecha_str.split('-')
                        if len(parts) == 3:
                            anio = int(parts[0])
                            mes = int(parts[1])
                            dia = int(parts[2])
                            
                            # Agregar campos calculados
                            new_row.extend([anio, mes, dia, fecha_str, f"{mes:02d}.{anio}"])
                
                transformed_data.append(new_row)
            
            return transformed_data
        except Exception as e:
            print(f"Error en transformación de datos: {str(e)}")
            return data

    def upsert_ericsson_counters(self, data):
        """Realiza UPSERT en la tabla TELCEL_EE_ERICSSONCOUNTERS.
        
        Args:
            data (list): Lista de registros transformados.
            
        Returns:
            dict: Resultado de la operación.
        """
        try:
            if not data:
                return {
                    'success': True,
                    'message': 'No hay datos para procesar',
                    'affected_rows': 0
                }

            # Construir query UPSERT
            upsert_query = f"""
            UPSERT {DB_CONFIG['schema']}.TELCEL_EE_ERICSSONCOUNTERS
            (FECHA, HORA, BTSNAME, IDBTSNAME, CONSUMEDENERGY, CONSUMEDENERGYACCUMULATED, 
             VOLTAGE, POWERCONSUMPTION, MINPOWERCONSUMPTION, MAXPOWERCONSUMPTION, 
             MIMOSLEEPOPPTIME, MIMOSLEEPTIME, CELLSLEEPFAILUECAP, CELLSLEEPTIME, 
             PROVEEDOR, TECNOLOGIA, OBJECTTYPE, ANIO, MES, DIA, Fecha_Txt, ANIOMES)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor = self.connection.cursor()
            cursor.executemany(upsert_query, data)
            affected_rows = cursor.rowcount
            cursor.close()
            
            return {
                'success': True,
                'message': f'UPSERT completado exitosamente. Filas afectadas: {affected_rows}',
                'affected_rows': affected_rows
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error en UPSERT: {str(e)}',
                'affected_rows': 0
            }

    def truncate_temp_table(self):
        """Trunca la tabla temporal TELCEL_EE_TEMPERICSSONCOUNTERS.
        
        Returns:
            dict: Resultado de la operación.
        """
        try:
            query = f"TRUNCATE TABLE {DB_CONFIG['schema']}.TELCEL_EE_TEMPERICSSONCOUNTERS"
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            
            return {
                'success': True,
                'message': 'Tabla temporal truncada exitosamente'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al truncar tabla temporal: {str(e)}'
            }

    def get_record_counts(self):
        """Obtiene el conteo de registros de las tablas principales.
        
        Returns:
            dict: Conteos de registros.
        """
        try:
            counts = {}
            tables = [
                'TELCEL_EE_TEMPERICSSONCOUNTERS',
                'TELCEL_EE_ERICSSONCOUNTERS'
            ]
            
            cursor = self.connection.cursor()
            for table in tables:
                query = f"SELECT COUNT(*) FROM {DB_CONFIG['schema']}.{table}"
                cursor.execute(query)
                count = cursor.fetchone()[0]
                counts[table] = count
            
            cursor.close()
            return {
                'success': True,
                'counts': counts
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al obtener conteos: {str(e)}',
                'counts': {}
            }