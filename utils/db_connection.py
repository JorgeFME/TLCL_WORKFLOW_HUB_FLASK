"""
Módulo para gestionar la conexión a la base de datos SAP HANA.
Proporciona funciones para establecer y cerrar conexiones.
"""

import hdbcli.dbapi
from utils.config import DB_CONFIG

class HanaConnection:
    """Clase para gestionar la conexión a SAP HANA."""
    
    def __init__(self):
        """Inicializa los atributos de conexión."""
        self.connection = None
        self.cursor = None
        self.config = DB_CONFIG
    
    def connect(self):
        """Establece la conexión a la base de datos SAP HANA.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        try:
            self.connection = hdbcli.dbapi.connect(
                address=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                currentSchema=self.config['schema']
            )
            self.cursor = self.connection.cursor()
            print("Conexión establecida exitosamente a SAP HANA.")
            return True
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            return False
    
    def close(self):
        """Cierra la conexión a la base de datos.
        
        Returns:
            bool: True si se cerró correctamente, False en caso contrario.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                print("Conexión cerrada exitosamente.")
            return True
        except Exception as e:
            print(f"Error al cerrar la conexión: {e}")
            return False