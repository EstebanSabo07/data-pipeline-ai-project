"""
db_config.py
============
Módulo de configuración de la base de datos PostgreSQL.
Lee las credenciales desde variables de entorno (.env) para evitar
exponer datos sensibles en el código fuente.

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
Modificado por: Ariana Víquez S.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

@dataclass
class DBConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

def get_db_config() -> DBConfig:
    """
    Carga la configuración de la base de datos desde variables de entorno.
    Busca el archivo .env en la carpeta de la Parte 1 de forma absoluta.
    """
    # Localizar la ruta de este archivo y subir un nivel para llegar a la raíz de parte1
    base_path = Path(__file__).parent.parent
    env_path = base_path / "config" / ".env"
    
    # Cargar el .env explícitamente desde esa ruta
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        # Si no existe en la carpeta config, intentar en la raíz del proyecto
        load_dotenv()

    # Leer variables
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    # Validación
    missing = []
    if not host: missing.append("DB_HOST")
    if not port: missing.append("DB_PORT")
    if not database: missing.append("DB_NAME")
    if not user: missing.append("DB_USER")

    if missing:
        raise ValueError(
            f"Faltan las siguientes variables de entorno: {missing}\n"
            f"Buscando en: {env_path.absolute()}\n"
            "Por favor, asegúrate de que el archivo .env exista y tenga las credenciales."
        )

    return DBConfig(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password if password else ""
    )
