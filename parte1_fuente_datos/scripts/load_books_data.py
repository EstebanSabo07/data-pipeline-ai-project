import pandas as pd
import logging
from pathlib import Path
import sys
from sqlalchemy import text

# Agregar rutas para importar db_connection
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
from scripts.db_connection import get_database_connection

logger = logging.getLogger("books.load")

def load_books_data(truncate_first=True):
    """
    Carga los archivos CSV de Amazon Books en PostgreSQL.
    """
    results = {"books_data": 0, "books_rating": 0}
    db = get_database_connection()
    engine = db.get_engine()
    
    raw_dir = BASE_DIR / "data" / "raw"
    files_to_load = [
        ("books_data.csv", "books_data"),
        ("Books_rating.csv", "books_rating")
    ]
    
    # Mapeo completo incluyendo variaciones comunes de Kaggle
    mapping = {
        'Title': 'title', 'title': 'title',
        'description': 'description', 'Description': 'description',
        'authors': 'authors', 'Authors': 'authors',
        'image': 'image', 'Image': 'image',
        'previewLink': 'preview_link', 'PreviewLink': 'preview_link',
        'publisher': 'publisher', 'Publisher': 'publisher',
        'publishedDate': 'published_date', 'PublishedDate': 'published_date',
        'infoLink': 'info_link', 'InfoLink': 'info_link',
        'categories': 'categories', 'Categories': 'categories',
        'ratingsCount': 'ratings_count', 'RatingsCount': 'ratings_count',
        'Id': 'id', 'id': 'id',
        'Price': 'price', 'price': 'price',
        'User_id': 'user_id', 'user_id': 'user_id',
        'profileName': 'profile_name', 'ProfileName': 'profile_name',
        'helpfulness': 'helpfulness', 'Helpfulness': 'helpfulness', # Corregido aqui
        'review/score': 'review_score', 'review_score': 'review_score',
        'review/time': 'review_time', 'review_time': 'review_time',
        'review/summary': 'review_summary', 'review_summary': 'review_summary',
        'review/text': 'review_text', 'Review_text': 'review_text'
    }

    try:
        for file_name, table_name in files_to_load:
            file_path = raw_dir / file_name
            if not file_path.exists():
                logger.error(f"Archivo no encontrado: {file_path}")
                continue
                
            logger.info(f"Cargando {file_name} en books.{table_name}...")
            
            if truncate_first:
                with engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE books.{table_name} CASCADE"))
                    conn.commit()

            chunk_size = 50000
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                # 1. Renombrar columnas
                chunk = chunk.rename(columns=mapping)
                
                # 2. Solo columnas que existen en nuestra DB
                if table_name == "books_data":
                    cols = ['title', 'description', 'authors', 'image', 'preview_link', 
                            'publisher', 'published_date', 'info_link', 'categories', 'ratings_count']
                else:
                    cols = ['id', 'title', 'price', 'user_id', 'profile_name', 
                            'helpfulness', 'review_score', 'review_time', 'review_summary', 'review_text']
                
                # 3. Validar cuáles columnas del mapeo realmente existen en este chunk
                existing_cols = [c for c in cols if c in chunk.columns]
                chunk = chunk[existing_cols]

                # 4. Insertar
                chunk.to_sql(name=table_name, schema='books', con=engine,
                             if_exists='append', index=False)
                
                if i % 10 == 0:
                    logger.info(f"   ... {(i+1)*chunk_size:,} registros procesados")
            
            with engine.connect() as conn:
                count = conn.execute(text(f"SELECT COUNT(*) FROM books.{table_name}")).scalar()
            
            results[table_name] = int(count)
            logger.info(f"Finalizado: {table_name} tiene {count:,} registros.")

        db.close()
        return results
        
    except Exception as e:
        logger.error(f"Error cargando datos: {e}")
        raise e