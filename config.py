"""
Módulo de configuración para la conexión a la base de datos SAP HANA.
Contiene las credenciales y parámetros de conexión usando variables de entorno.
"""
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

def get_db_config():
    """
    Obtiene la configuración de la base de datos desde variables de entorno.
    """
    # Configuración desde variables de entorno
    # IMPORTANTE: Las credenciales DEBEN estar en el archivo .env (local) 
    # o configuradas como variables de entorno en Cloud Foundry
    host = os.getenv('HANA_HOST')
    port = os.getenv('HANA_PORT', '443')
    user = os.getenv('HANA_USER')
    password = os.getenv('HANA_PASSWORD')
    schema = os.getenv('HANA_SCHEMA')
    
    # Validar que todas las variables requeridas estén presentes
    if not all([host, user, password, schema]):
        missing_vars = []
        if not host: missing_vars.append('HANA_HOST')
        if not user: missing_vars.append('HANA_USER')
        if not password: missing_vars.append('HANA_PASSWORD')
        if not schema: missing_vars.append('HANA_SCHEMA')
        
        raise ValueError(
            f"Variables de entorno faltantes: {', '.join(missing_vars)}. "
            f"Por favor, configura las variables de entorno necesarias."
        )
    
    return {
        'host': host,
        'port': int(port),
        'user': user,
        'password': password,
        'schema': schema
    }

# Configuración de la conexión a SAP HANA
DB_CONFIG = get_db_config()