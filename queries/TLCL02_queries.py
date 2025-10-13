from sqlite3 import Cursor
from utils.config import DB_CONFIG

class TLCL02Queries:

    def __init__(self, connection):
        self.connection = connection

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
            cursor.execute(query)
            columns = [row[0] for row in cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error al obtener columnas de la tabla {table_name}")
            return None

    def get_temp_kpi_data(self):
        try:
            cursor = self.connection.cursor
            columns = self.get_table_columns('TELCEL_EE_TEMPKPI')
            if not columns:
                return None

            query = f"SELECT * FROM \"{DB_CONFIG['schema']}\".\"TELCEL_EE_TEMPKPI\""
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
            print(f"Error al obtener datos de KPI temporal")
            return None


