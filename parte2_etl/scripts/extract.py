'''
extraccion de los datos
Autor: Ariana Víquez
'''

import logging
import sys
from pathlib import Path
import pandas as pd

# Configurar rutas relativas al archivo actual
BASE_DIR = Path(__file__).parent.parent
LOG_DIR = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True) # Crea la carpeta si no existe

# Agregar Parte 1 al path para importar la conexión
sys.path.insert(0, str(BASE_DIR.parent / "parte1_fuente_datos"))

from scripts.db_connection import get_database_connection

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_data():
    """Extrae datos desde PostgreSQL (amazon_books_db)."""
    logger.info("=== INICIO EXTRACCIÓN ===")
    
    try:
        db = get_database_connection()
        engine = db.get_engine()
        
        logger.info("Extrayendo tabla books.books_data...")
        df_books = pd.read_sql("SELECT * FROM books.books_data", engine)
        
        logger.info("Extrayendo tabla books.books_rating (limitado a 100k para eficiencia)...")
        # Usamos un LIMIT inicial para que el proceso no sea eterno en la primera prueba
        df_ratings = pd.read_sql("SELECT * FROM books.books_rating LIMIT 100000", engine)
        
        logger.info(f"Extracción exitosa: {len(df_books)} libros y {len(df_ratings)} reseñas.")
        db.close()
        return df_books, df_ratings
        
    except Exception as e:
        logger.error(f"Error en la extracción: {e}")
        raise e

if __name__ == "__main__":
    extract_data()