'''
Carga de los datos
Autor: Ariana Víquez
'''
import logging
import pandas as pd
from pathlib import Path
import sys

# Configurar rutas para acceder a la base de datos de la Parte 1
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR.parent / "parte1_fuente_datos"))

from scripts.db_connection import get_database_connection

logger = logging.getLogger(__name__)

def load_data(df_final):
    """
    Carga los datos transformados en una nueva tabla de PostgreSQL 
    y guarda una copia en formato CSV.
    """
    logger.info("=== INICIO CARGA ===")
    
    try:
        # 1. Guardar en CSV (Capa Curated)
        curated_dir = BASE_DIR / "data" / "curated"
        curated_dir.mkdir(parents=True, exist_ok=True)
        
        csv_path = curated_dir / "amazon_books_curated.csv"
        df_final.to_csv(csv_path, index=False)
        logger.info(f"Datos guardados en CSV: {csv_path}")
        
        # 2. Guardar en PostgreSQL
        db = get_database_connection()
        engine = db.get_engine()
        
        # Creamos la tabla en el schema 'books'
        logger.info("Cargando datos en PostgreSQL (tabla books.curated_books_reviews)...")
        df_final.to_sql(
            name='curated_books_reviews', 
            con=engine, 
            schema='books', 
            if_exists='replace', 
            index=False
        )
        
        logger.info("Carga en base de datos completada con éxito.")
        db.close()
        
    except Exception as e:
        logger.error(f"Error en la fase de carga: {e}")
        raise e

if __name__ == "__main__":
    # Prueba rápida con un DataFrame vacío si se ejecuta solo
    load_data(pd.DataFrame())