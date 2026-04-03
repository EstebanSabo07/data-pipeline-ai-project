'''
transformacion de los datos
Autor: Ariana Víquez
'''

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def transform_data(df_books, df_ratings):
    """
    Limpia y une los DataFrames de libros y reseñas.
    """
    logger.info("=== INICIO TRANSFORMACIÓN ===")
    
    try:
        # 1. Limpieza básica de libros
        # Eliminar duplicados por título
        df_books = df_books.drop_duplicates(subset=['title'])
        
        # 2. Limpieza de reseñas
        # Convertir review_time (Unix) a formato fecha legible
        if 'review_time' in df_ratings.columns:
            df_ratings['review_date'] = pd.to_datetime(df_ratings['review_time'], unit='s')
        
        # 3. Unir DataFrames (Inner Join por título)
        logger.info("Realizando join entre libros y reseñas...")
        df_merged = pd.merge(df_ratings, df_books, on='title', how='inner')
        
        # 4. Manejo de valores nulos en columnas críticas
        df_merged['review_score'] = df_merged['review_score'].fillna(0)
        df_merged['categories'] = df_merged['categories'].fillna('Unknown')
        
        # 5. Selección de columnas finales para el modelo de IA
        columns_to_keep = [
            'title', 'user_id', 'review_score', 'review_summary', 
            'review_text', 'review_date', 'authors', 'categories', 'publisher'
        ]
        
        # Solo mantener las columnas que realmente existen tras el join
        final_cols = [c for c in columns_to_keep if c in df_merged.columns]
        df_final = df_merged[final_cols]
        
        logger.info(f"Transformación completada. Registros finales: {len(df_final)}")
        return df_final
        
    except Exception as e:
        logger.error(f"Error en la transformación: {e}")
        raise e