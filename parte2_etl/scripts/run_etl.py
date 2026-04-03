'''
orquestacion de la perte 2 (etl)
Autor: Ariana Víquez
'''
import logging
import sys
from datetime import datetime
from pathlib import Path

# Configurar rutas
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

from extract import extract_data
from transform import transform_data
from load import load_data

# Configuración de Logging
LOG_DIR = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'etl_master.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_etl():
    """Orquestador principal del proceso ETL."""
    logger.info("="*60)
    logger.info("  PIPELINE ETL - PARTE 2 - AMAZON BOOKS REVIEWS")
    logger.info(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    try:
        # 1. EXTRAER
        print("\n>>> PASO 1: EXTRACCION")
        df_books, df_ratings = extract_data()
        
        # 2. TRANSFORMAR
        print("\n>>> PASO 2: TRANSFORMACION")
        df_final = transform_data(df_books, df_ratings)
        
        # 3. CARGAR
        print("\n>>> PASO 3: CARGA")
        load_data(df_final)
        
        logger.info("="*60)
        logger.info("  PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("="*60)
        
    except Exception as e:
        # Eliminado el emoji para evitar errores de consola
        logger.error(f"El Pipeline fallo: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()