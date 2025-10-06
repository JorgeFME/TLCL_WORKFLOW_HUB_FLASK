"""
Módulo para gestionar las consultas SQL específicas del proceso SITE.
Contiene todas las queries relacionadas con TELCEL_EE_SITE.
"""

from config import DB_CONFIG

class COBCENQueries:
    """Clase para gestionar las consultas específicas del proceso SITE."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection
    
    def get_site_data(self):
        """Obtiene el primer registro de la tabla TELCEL_EE_SITE.
        
        Returns:
            tuple: (columnas, datos) donde columnas es una lista de nombres de columnas
                   y datos es una lista con el primer registro.
            None: Si ocurre un error o no hay datos.
        """
        try:
            cursor = self.connection.cursor
            query = f'SELECT TOP 1 * FROM "{DB_CONFIG["schema"]}"."TELCEL_EE_SITE"'
            cursor.execute(query)
            
            # Obtener nombres de columnas
            columns = [desc[0] for desc in cursor.description]
            
            # Obtener el primer registro
            result = cursor.fetchone()
            
            if result:
                return columns, [result]
            else:
                return columns, []
                
        except Exception as e:
            print(f"Error al obtener datos de TELCEL_EE_SITE: {e}")
            return None
    
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