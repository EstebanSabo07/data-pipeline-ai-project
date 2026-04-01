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
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.db_config import DatabaseConfig, get_db_config

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist.db_connection")


# ============================================================
# CLASE PRINCIPAL: DatabaseConnection
# ============================================================
class DatabaseConnection:
    """
    Gestiona la conexión a PostgreSQL para el pipeline Olist.
    Soporta dos modos de uso:
      1. psycopg2 — para operaciones SQL directas y COPY masivo
      2. SQLAlchemy — para integración con pandas (read_sql / to_sql)
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Inicializa el gestor de conexión.

        Args:
            config: DatabaseConfig. Si no se provee, lo lee del .env automáticamente.
        """
        self.config = config or get_db_config()
        self._engine: Optional[Engine] = None
        logger.info(f"DatabaseConnection inicializado: {self.config}")

    # ----------------------------------------------------------
    # SQLALCHEMY ENGINE (para pandas)
    # ----------------------------------------------------------
    def get_engine(self) -> Engine:
        """
        Retorna (y cachea) el engine de SQLAlchemy.
        Ideal para usar con pandas.read_sql() y DataFrame.to_sql().

        Returns:
            sqlalchemy.engine.Engine listo para usar.
        """
        if self._engine is None:
            logger.info("Creando engine SQLAlchemy...")
            self._engine = create_engine(
                self.config.connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_pre_ping=True,  # Verifica conexión antes de usarla
                echo=False,          # True para ver SQL generado (debug)
            )
            # Verificar que la conexión funciona
            try:
                with self._engine.connect() as conn:
                    result = conn.execute(text("SELECT version()"))
                    version = result.fetchone()[0]
                    logger.info(f"✅ Conexión SQLAlchemy exitosa | {version[:50]}...")
            except OperationalError as e:
                logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
                raise

        return self._engine

    # ----------------------------------------------------------
    # PSYCOPG2 CONTEXT MANAGER (para SQL directo)
    # ----------------------------------------------------------
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Context manager para conexión psycopg2.
        Garantiza commit/rollback y cierre automático.

        Uso:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT ...")

        Yields:
            psycopg2.connection con autocommit=False
        """
        conn = None
        try:
            conn = psycopg2.connect(self.config.dsn)
            conn.autocommit = False
            logger.debug("Conexión psycopg2 abierta")
            yield conn
            conn.commit()
            logger.debug("Transacción committed")
        except Exception as e:
            if conn:
                conn.rollback()
                logger.error(f"Rollback ejecutado por error: {e}")
            raise
        finally:
            if conn and not conn.closed:
                conn.close()
                logger.debug("Conexión psycopg2 cerrada")

    @contextmanager
    def get_cursor(
        self, cursor_factory=psycopg2.extras.RealDictCursor
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        """
        Context manager para cursor psycopg2.
        Retorna RealDictCursor por defecto (filas como diccionarios).

        Uso:
            with db.get_cursor() as cur:
                cur.execute("SELECT * FROM olist.orders LIMIT 5")
                rows = cur.fetchall()

        Yields:
            psycopg2 cursor
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=cursor_factory) as cur:
                yield cur

    # ----------------------------------------------------------
    # MÉTODOS DE UTILIDAD
    # ----------------------------------------------------------
    def test_connection(self) -> bool:
        """
        Prueba la conectividad a la base de datos.

        Returns:
            True si la conexión es exitosa, False en caso contrario.
        """
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT current_database(), current_user, version()")
                row = cur.fetchone()
                logger.info(
                    f"✅ Conexión exitosa | "
                    f"DB: {row['current_database']} | "
                    f"Usuario: {row['current_user']}"
                )
                return True
        except Exception as e:
            logger.error(f"❌ Falló la conexión: {e}")
            return False

    def execute_sql_file(self, sql_file_path: str) -> bool:
        """
        Ejecuta un archivo .sql completo contra la base de datos.
        Útil para correr los scripts DDL desde Python.

        Args:
            sql_file_path: Ruta al archivo .sql

        Returns:
            True si se ejecutó sin errores.
        """
        sql_path = Path(sql_file_path)
        if not sql_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo SQL: {sql_file_path}")

        logger.info(f"Ejecutando: {sql_path.name}")
        sql_content = sql_path.read_text(encoding="utf-8")

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_content)

        logger.info(f"✅ {sql_path.name} ejecutado exitosamente")
        return True

    def get_table_counts(self) -> dict:
        """
        Retorna el conteo de registros de todas las tablas del schema olist.
        Útil para verificar la carga de datos.

        Returns:
            Diccionario {nombre_tabla: cantidad_registros}
        """
        tables = [
            "customers", "geolocation", "sellers", "products",
            "product_category_name_translation", "orders",
            "order_items", "order_payments", "order_reviews"
        ]

        counts = {}
        with self.get_cursor() as cur:
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) AS cnt FROM olist.{table}")
                    counts[table] = cur.fetchone()["cnt"]
                except Exception as e:
                    counts[table] = f"ERROR: {e}"

        return counts

    def close(self):
        """Cierra el engine SQLAlchemy y libera recursos."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Engine SQLAlchemy cerrado")


# ============================================================
# INSTANCIA GLOBAL (singleton ligero — importable por otros módulos)
# ============================================================
def get_database_connection() -> DatabaseConnection:
    """
    Factory function para obtener una instancia de DatabaseConnection.
    Los demás módulos del pipeline deben importar esta función.

    Uso en otros scripts:
        from scripts.db_connection import get_database_connection
        db = get_database_connection()
        engine = db.get_engine()

    Returns:
        DatabaseConnection configurada con las variables de entorno del .env
    """
    return DatabaseConnection()


# ============================================================
# EJECUCIÓN DIRECTA (para prueba rápida de la conexión)
# ============================================================
if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  TEST DE CONEXIÓN — OLIST DB")
    print("=" * 55)

    db = get_database_connection()

    # Prueba 1: Conectividad básica
    print("\n[1/3] Probando conectividad psycopg2...")
    connected = db.test_connection()

    if connected:
        # Prueba 2: Engine SQLAlchemy
        print("\n[2/3] Probando engine SQLAlchemy...")
        engine = db.get_engine()
        print(f"      Engine creado: {engine.url.drivername}")

        # Prueba 3: Conteo de tablas
        print("\n[3/3] Conteo de registros por tabla:")
        counts = db.get_table_counts()
        for tabla, n in counts.items():
            print(f"      {tabla:<45} {n:>10,} registros")

        print("\n✅ Todos los tests pasaron. Base de datos lista.")
    else:
        print("\n❌ No se pudo conectar. Verificar el archivo .env")
        sys.exit(1)
