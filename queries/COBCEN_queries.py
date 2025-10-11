"""
Módulo para gestionar las consultas SQL específicas del proceso COBCEN.
Incluye utilidades para ejecutar el script SQL de merges definido en queryCobcen.sql.
"""

import os
from utils.sql_runner import SqlRunner

class COBCENQueries:
    """Clase para gestionar las consultas específicas del proceso COBCEN."""
    
    def __init__(self, connection):
        """Inicializa la clase con una conexión existente.
        
        Args:
            connection: Objeto de conexión a la base de datos.
        """
        self.connection = connection
    
    # Eliminado: métodos de preview y utilidades de columnas no utilizados

    def run_cobcen_sql_script(self):
        """Ejecuta el script SQL de COBCEN ubicado en queries/COBCEN_merge.sql.

        Ejecuta las sentencias MERGE para SITE, ADDRESS, GATEWAY y SIM.

        Returns:
            dict: Resumen de ejecución con número de sentencias ejecutadas y estado.
        """
        try:
            # Construir ruta absoluta del archivo SQL
            base_dir = os.path.dirname(os.path.abspath(__file__))
            sql_path = os.path.join(base_dir, 'COBCEN_merge.sql')

            # Usar el runner común para ejecutar el archivo
            runner = SqlRunner(self.connection)
            res = runner.execute_sql_file(sql_path, commit_mode='end', stop_on_error=True)

            # Ajustar mensaje y detalles para COBCEN
            if res['success']:
                res['message'] = 'Script COBCEN ejecutado correctamente'
            else:
                res['message'] = f"Error al ejecutar script COBCEN: {res.get('message', 'desconocido')}"
            return res
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al ejecutar script COBCEN: {str(e)}',
                'details': None
            }