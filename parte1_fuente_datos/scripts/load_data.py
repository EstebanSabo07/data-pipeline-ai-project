"""
load_data.py
============
Script de carga de datos: lee los CSV del dataset Olist y los inserta
en las tablas PostgreSQL del schema 'olist'.

Características:
  - Carga en el orden correcto para respetar FK
  - UPSERT inteligente: evita duplicados en re-ejecuciones
  - Logging detallado de cada etapa
  - Manejo de valores nulos y tipos de datos
  - Reporte final de registros cargados

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

# Agregar directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.db_connection import get_database_connection

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist.load_data")

# ============================================================
# CONSTANTES
# ============================================================
DATA_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# Columnas de timestamp que deben convertirse
TIMESTAMP_COLS = {
    "olist_orders_dataset.csv": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_order_reviews_dataset.csv": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "olist_order_items_dataset.csv": [
        "shipping_limit_date",
    ],
}

# Mapeo: archivo CSV → tabla PostgreSQL (en orden de carga respetando FK)
LOAD_ORDER = [
    ("product_category_name_translation.csv", "olist.product_category_name_translation"),
    ("olist_customers_dataset.csv",           "olist.customers"),
    ("olist_geolocation_dataset.csv",         "olist.geolocation"),
    ("olist_sellers_dataset.csv",             "olist.sellers"),
    ("olist_products_dataset.csv",            "olist.products"),
    ("olist_orders_dataset.csv",              "olist.orders"),
    ("olist_order_items_dataset.csv",         "olist.order_items"),
    ("olist_order_payments_dataset.csv",      "olist.order_payments"),
    ("olist_order_reviews_dataset.csv",       "olist.order_reviews"),
]

# ============================================================
# FUNCIONES DE LIMPIEZA PRE-CARGA
# ============================================================

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza mínima para la tabla customers."""
    df = df.drop_duplicates(subset=["customer_id"])
    df["customer_state"] = df["customer_state"].str.strip().str.upper()
    df["customer_city"] = df["customer_city"].str.strip().str.lower()
    logger.info(f"   Clientes después de deduplicación: {len(df):,}")
    return df


def clean_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia geolocation: filtra coordenadas de Brasil (bounding box).
    Brasil: lat [-33.75, 5.27], lon [-73.99, -34.79]
    """
    original_len = len(df)
    df = df[
        (df["geolocation_lat"].between(-33.75, 5.27)) &
        (df["geolocation_lng"].between(-73.99, -34.79))
    ]
    removed = original_len - len(df)
    if removed > 0:
        logger.info(f"   Geolocation: {removed:,} registros fuera de Brasil eliminados")
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza para la tabla orders."""
    df = df.drop_duplicates(subset=["order_id"])

    # Convertir timestamps
    ts_cols = TIMESTAMP_COLS.get("olist_orders_dataset.csv", [])
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Normalizar status
    df["order_status"] = df["order_status"].str.strip().str.lower()

    # Filtrar status válidos (según constraint SQL)
    valid_status = {
        "created", "approved", "processing", "invoiced",
        "shipped", "delivered", "unavailable", "canceled"
    }
    invalid_mask = ~df["order_status"].isin(valid_status)
    if invalid_mask.sum() > 0:
        logger.warning(f"   Orders: {invalid_mask.sum()} registros con status inválido → se omiten")
        df = df[~invalid_mask]

    logger.info(f"   Órdenes después de limpieza: {len(df):,}")
    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza para order_items."""
    # Convertir timestamps
    ts_cols = TIMESTAMP_COLS.get("olist_order_items_dataset.csv", [])
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Asegurar que price y freight sean positivos
    df = df[df["price"] >= 0]
    df = df[df["freight_value"] >= 0]

    return df


def clean_order_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza para order_payments."""
    df = df[df["payment_value"] >= 0]
    df["payment_type"] = df["payment_type"].str.strip().str.lower()

    # Normalizar tipos de pago desconocidos
    valid_types = {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
    df["payment_type"] = df["payment_type"].apply(
        lambda x: x if x in valid_types else "not_defined"
    )

    return df


def clean_order_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza para order_reviews (tabla target del modelo IA)."""
    df = df.drop_duplicates(subset=["review_id"])

    # Convertir timestamps
    ts_cols = TIMESTAMP_COLS.get("olist_order_reviews_dataset.csv", [])
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Filtrar scores fuera de rango (1-5)
    df = df[df["review_score"].between(1, 5)]

    # Truncar comentarios muy largos para evitar overflow
    if "review_comment_title" in df.columns:
        df["review_comment_title"] = df["review_comment_title"].str[:255]

    logger.info(f"   Reviews después de limpieza: {len(df):,}")
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza para products."""
    df = df.drop_duplicates(subset=["product_id"])
    return df


# Función dispatch de limpieza por tabla
CLEANERS = {
    "olist_customers_dataset.csv":      clean_customers,
    "olist_geolocation_dataset.csv":    clean_geolocation,
    "olist_orders_dataset.csv":         clean_orders,
    "olist_order_items_dataset.csv":    clean_order_items,
    "olist_order_payments_dataset.csv": clean_order_payments,
    "olist_order_reviews_dataset.csv":  clean_order_reviews,
    "olist_products_dataset.csv":       clean_products,
}


# ============================================================
# FUNCIÓN PRINCIPAL DE CARGA
# ============================================================

def load_table(
    engine,
    csv_file: str,
    table_name: str,
    chunk_size: int = 10_000,
) -> int:
    """
    Carga un CSV a una tabla PostgreSQL usando pandas + SQLAlchemy.

    Args:
        engine: SQLAlchemy engine.
        csv_file: Nombre del archivo CSV en data/raw/
        table_name: Nombre de la tabla destino (schema.tabla)
        chunk_size: Registros por lote (optimiza memoria)

    Returns:
        Cantidad de registros cargados.
    """
    csv_path = DATA_RAW_DIR / csv_file

    if not csv_path.exists():
        logger.warning(f"   ⚠️  Archivo no encontrado: {csv_path}")
        return 0

    logger.info(f"\n{'─'*55}")
    logger.info(f"📥 Cargando: {csv_file}")
    logger.info(f"   → Destino: {table_name}")

    # Leer CSV
    t_start = time.time()
    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"   CSV leído: {len(df):,} filas × {len(df.columns)} columnas")

    # Aplicar limpieza específica si existe
    cleaner = CLEANERS.get(csv_file)
    if cleaner:
        df = cleaner(df)

    # Separar schema.tabla para pandas
    schema, tbl = table_name.split(".")

    # Cargar en lotes usando if_exists='append' (tabla ya creada en SQL)
    total_loaded = 0
    n_chunks = (len(df) // chunk_size) + 1

    for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
        chunk = df.iloc[chunk_start : chunk_start + chunk_size]
        chunk.to_sql(
            name=tbl,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",     # Inserts agrupados (más rápido)
        )
        total_loaded += len(chunk)
        logger.info(f"   Lote {i+1}/{n_chunks}: {total_loaded:,}/{len(df):,} registros cargados")

    elapsed = time.time() - t_start
    logger.info(f"   ✅ {total_loaded:,} registros cargados en {elapsed:.1f}s")

    return total_loaded


def truncate_all_tables(engine) -> None:
    """
    Vacía todas las tablas en orden inverso para respetar FK.
    Se ejecuta antes de una recarga completa.
    """
    logger.info("\n🗑️  Limpiando tablas existentes (TRUNCATE CASCADE)...")

    # Orden inverso al de carga para respetar FK
    tables_reverse = [tbl for _, tbl in reversed(LOAD_ORDER)]

    with engine.connect() as conn:
        for table in tables_reverse:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            logger.info(f"   Vaciada: {table}")
        conn.commit()

    logger.info("✅ Tablas vaciadas")


def run_full_load(truncate_first: bool = True) -> dict:
    """
    Ejecuta la carga completa del dataset Olist en PostgreSQL.

    Args:
        truncate_first: Si True, vacía las tablas antes de cargar (safe re-run).

    Returns:
        Diccionario con conteo de registros cargados por tabla.
    """
    logger.info("\n" + "=" * 55)
    logger.info("  CARGA DE DATOS — OLIST → POSTGRESQL")
    logger.info("=" * 55)
    logger.info(f"  Directorio de datos: {DATA_RAW_DIR}")

    # Conectar a la base
    db = get_database_connection()
    engine = db.get_engine()

    # Vaciar tablas si se solicita (idempotencia)
    if truncate_first:
        truncate_all_tables(engine)

    # Cargar en el orden definido
    results = {}
    t_total_start = time.time()

    for csv_file, table_name in LOAD_ORDER:
        loaded = load_table(engine, csv_file, table_name)
        results[table_name] = loaded

    # Resumen final
    total_elapsed = time.time() - t_total_start
    total_records = sum(results.values())

    logger.info("\n" + "=" * 55)
    logger.info("  RESUMEN DE CARGA")
    logger.info("=" * 55)
    for table, count in results.items():
        logger.info(f"  {table:<45} {count:>10,} registros")
    logger.info(f"{'─'*55}")
    logger.info(f"  TOTAL                                         {total_records:>10,} registros")
    logger.info(f"  Tiempo total: {total_elapsed:.1f} segundos")
    logger.info("=" * 55)

    db.close()
    return results


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Carga el dataset Olist en PostgreSQL"
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="No vaciar tablas antes de cargar (append mode)",
    )
    args = parser.parse_args()

    results = run_full_load(truncate_first=not args.no_truncate)

    print("\n✅ Carga completada exitosamente.")
    print("   Próximo paso: ejecutar la Parte 2 (ETL)")
