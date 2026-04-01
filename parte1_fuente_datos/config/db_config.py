"""
db_config.py
============
Módulo de configuración de la base de datos PostgreSQL.
Lee las credenciales desde variables de entorno (.env) para evitar
exponer datos sensibles en el código fuente.

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Cargar variables del archivo .env (ubicado en parte1_fuente_datos/)
load_dotenv()


@dataclass
class DatabaseConfig:
    """
    Configuración inmutable de conexión a PostgreSQL.
    Se construye desde variables de entorno.
    """
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        """
        Retorna el string de conexión SQLAlchemy (psycopg2 driver).
        Formato: postgresql+psycopg2://user:password@host:port/database
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def dsn(self) -> str:
        """
        Retorna el DSN de psycopg2 (conexión directa sin SQLAlchemy).
        """
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password}"
        )

    def __repr__(self) -> str:
        """Representación segura (oculta la contraseña)."""
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, "
            f"database={self.database}, user={self.user}, password=***)"
        )


def get_db_config() -> DatabaseConfig:
    """
    Construye y retorna la configuración de base de datos
    leyendo desde las variables de entorno.

    Returns:
        DatabaseConfig: Objeto con todas las credenciales de conexión.

    Raises:
        ValueError: Si alguna variable de entorno obligatoria no está definida.
    """
    # DB_PASSWORD es opcional (Postgres.app no usa contraseña por defecto)
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise ValueError(
            f"Faltan las siguientes variables de entorno: {missing}\n"
            "Por favor, copia el archivo config/.env.example como .env "
            "y completa tus credenciales."
        )

    return DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "olist_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )
