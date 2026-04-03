"""
run_parte1.py
=============
Script maestro de la Parte 1: orquesta todos los pasos del pipeline
de fuente de datos en el orden correcto.

Ejecución:
    python scripts/run_parte1.py

Pasos que ejecuta:
    1. Test de conexión a PostgreSQL
    2. Verificación del dataset Amazon Books Reviews (9 CSV en data/raw/)
    3. Creación del esquema de base de datos
    4. Carga de datos (9 tablas)
    5. Verificación post-carga

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
Modificado por:Ariana Víquez S.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# Rutas
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

# Configuración de Logging sin emojis
LOG_DIR = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOG_DIR / f"parte1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, "w", encoding='utf-8'),
    ],
)
logger = logging.getLogger("books.master")

def step_1_test_connection():
    logger.info("PASO 1: TEST DE CONEXION")
    from scripts.db_connection import get_database_connection
    db = get_database_connection()
    if not db.test_connection():
        return False
    logger.info("Exito: Conexion establecida")
    return True

def step_2_verify_dataset():
    logger.info("PASO 2: VERIFICACION DE ARCHIVOS CSV")
    raw_dir = BASE_DIR / "data" / "raw"
    files = ["books_data.csv", "Books_rating.csv"]
    for f in files:
        if not (raw_dir / f).exists():
            logger.error(f"Falta el archivo: {f}")
            return False
        logger.info(f"Encontrado: {f}")
    return True

def step_3_create_schema():
    logger.info("PASO 3: CREACION DE TABLAS SQL")
    import psycopg2
    from config.db_config import get_db_config
    cfg = get_db_config()
    try:
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port, dbname=cfg.database,
            user=cfg.user, password=cfg.password
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            # Borramos las tablas si existen para recrearlas con los tipos correctos
            cur.execute("DROP TABLE IF EXISTS books.books_data CASCADE")
            cur.execute("DROP TABLE IF EXISTS books.books_rating CASCADE")
            
            cur.execute("CREATE SCHEMA IF NOT EXISTS books")
            
            # Usamos TEXT para evitar el error de StringDataRightTruncation
            cur.execute("""
                CREATE TABLE books.books_data (
                    title TEXT, 
                    description TEXT, 
                    authors TEXT, 
                    image TEXT, 
                    preview_link TEXT, 
                    publisher TEXT, 
                    published_date TEXT, 
                    info_link TEXT, 
                    categories TEXT, 
                    ratings_count NUMERIC
                )""")
                
            cur.execute("""
                CREATE TABLE books.books_rating (
                    id TEXT, 
                    title TEXT, 
                    price NUMERIC, 
                    user_id TEXT, 
                    profile_name TEXT, 
                    helpfulness TEXT, 
                    review_score NUMERIC, 
                    review_time BIGINT, 
                    review_summary TEXT, 
                    review_text TEXT
                )""")
        conn.close()
        logger.info("Exito: Esquema y tablas recreados con tipo TEXT")
        return True
    except Exception as e:
        logger.error(f"Error SQL: {e}")
        return False
    
    
def step_4_load_data():
    logger.info("PASO 4: CARGA DE DATOS (CSV -> SQL)")
    from scripts.load_books_data import load_books_data
    results = load_books_data(truncate_first=True)
    total = sum(results.values())
    logger.info(f"Exito: {total} registros cargados")
    return True

def step_5_verify():
    logger.info("PASO 5: VERIFICACION POST-CARGA")
    from scripts.verify_load import run_verification
    return run_verification()

def main():
    print("-" * 50)
    print("EJECUTANDO PARTE 1 - AMAZON BOOKS")
    print("-" * 50)
    
    steps = [
        ("Conexion", step_1_test_connection),
        ("Dataset", step_2_verify_dataset),
        ("Schema", step_3_create_schema),
        ("Carga", step_4_load_data),
        ("Verificacion", step_5_verify)
    ]
    
    for name, fn in steps:
        if not fn():
            logger.error(f"Pipeline detenido en: {name}")
            sys.exit(1)
            
    print("-" * 50)
    print("PARTE 1 COMPLETADA")
    print("-" * 50)

if __name__ == "__main__":
    main()