"""
db_connection.py
================
Módulo de conexión a PostgreSQL — reutilizable por todas las partes del pipeline.
Provee conexión tanto con psycopg2 (raw SQL) como con SQLAlchemy (ORM/pandas).
Incluye manejo de errores, logging y context managers para uso seguro.

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_db_config

logger = logging.getLogger("books.db_connection")

class DatabaseConnection:
    """Maneja la conexión a PostgreSQL usando SQLAlchemy."""
    
    def __init__(self, config=None):
        self.config = config or get_db_config()
        self.engine = self._create_engine()

    def _create_engine(self):
        """Crea el engine de SQLAlchemy con soporte para caracteres especiales."""
        conn_str = (
            f"postgresql+psycopg2://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )
        # Agregamos client_encoding para evitar errores de decodificación de mensajes del sistema
        return create_engine(
            conn_str,
            connect_args={'client_encoding': 'utf8'}
        )

    def test_connection(self) -> bool:
        """Prueba si la conexión es exitosa."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Error de conexión: {e}")
            return False

    def get_engine(self):
        return self.engine

    def close(self):
        if self.engine:
            self.engine.dispose()

def get_database_connection():
    return DatabaseConnection()
