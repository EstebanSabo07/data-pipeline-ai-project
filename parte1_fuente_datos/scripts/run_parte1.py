"""
run_parte1.py
=============
Script maestro de la Parte 1: orquesta todos los pasos del pipeline
de fuente de datos en el orden correcto.

Ejecución:
    python scripts/run_parte1.py

Pasos que ejecuta:
    1. Test de conexión a PostgreSQL
    2. Verificación del dataset Olist (9 CSV en data/raw/)
    3. Creación del esquema de base de datos
    4. Carga de datos (9 tablas)
    5. Verificación post-carga

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import logging
import sys
import time
from pathlib import Path

# ============================================================
# LOGGING — configuración central para toda la Parte 1
# ============================================================
LOG_DIR = Path(__file__).parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

from datetime import datetime
log_filename = LOG_DIR / f"parte1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),           # Consola
        logging.FileHandler(log_filename, "w"),      # Archivo de log
    ],
)
logger = logging.getLogger("olist.parte1.master")

# Agregar directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))


def step_1_test_connection() -> bool:
    """Paso 1: Verificar conectividad con PostgreSQL."""
    logger.info("\n" + "━" * 60)
    logger.info("  PASO 1/5: TEST DE CONEXIÓN A POSTGRESQL")
    logger.info("━" * 60)

    from scripts.db_connection import get_database_connection
    db = get_database_connection()
    connected = db.test_connection()

    if not connected:
        logger.error("❌ No se pudo conectar. Verificar .env y que PostgreSQL esté activo.")
        return False

    logger.info("✅ Paso 1 completado: Conexión exitosa")
    return True


def step_2_verify_dataset() -> bool:
    """Paso 2: Verificar que el dataset Olist está disponible en data/raw/."""
    logger.info("\n" + "━" * 60)
    logger.info("  PASO 2/5: VERIFICACIÓN DEL DATASET OLIST")
    logger.info("━" * 60)

    from scripts.download_dataset import download_dataset
    success = download_dataset()

    if not success:
        logger.error(
            "❌ Dataset no encontrado en data/raw/\n"
            "   Descarga manualmente desde:\n"
            "   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\n"
            "   y extrae los 9 CSV en: parte1_fuente_datos/data/raw/"
        )
        return False

    logger.info("✅ Paso 2 completado: Dataset verificado (9/9 archivos)")
    return True


def step_3_create_schema() -> bool:
    """Paso 3: Crear el schema olist y todas las tablas en PostgreSQL."""
    logger.info("\n" + "━" * 60)
    logger.info("  PASO 3/5: CREACIÓN DEL ESQUEMA EN POSTGRESQL")
    logger.info("━" * 60)

    import psycopg2
    from config.db_config import get_db_config

    cfg = get_db_config()

    def exec_single(conn, sql, description=""):
        """Ejecuta un statement en su propia transacción independiente."""
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql)
            return True
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["already exists", "duplicate"]):
                return True  # OK, ya existe
            logger.warning(f"   ⚠ {description}: {str(e)[:80]}")
            return False

    try:
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port, dbname=cfg.database,
            user=cfg.user, password=cfg.password
        )
        conn.autocommit = True

        logger.info("   Creando schema olist...")
        exec_single(conn, "CREATE SCHEMA IF NOT EXISTS olist AUTHORIZATION CURRENT_USER", "CREATE SCHEMA")
        exec_single(conn, "SET search_path TO olist, public", "SET search_path")

        logger.info("   Creando tablas...")

        tables_sql = [
            ("product_category_name_translation", """
                CREATE TABLE IF NOT EXISTS olist.product_category_name_translation (
                    product_category_name         VARCHAR(100) NOT NULL,
                    product_category_name_english VARCHAR(100) NOT NULL,
                    CONSTRAINT pk_product_category PRIMARY KEY (product_category_name)
                )"""),
            ("customers", """
                CREATE TABLE IF NOT EXISTS olist.customers (
                    customer_id              VARCHAR(50)  NOT NULL,
                    customer_unique_id       VARCHAR(50)  NOT NULL,
                    customer_zip_code_prefix VARCHAR(8)   NOT NULL,
                    customer_city            VARCHAR(100) NOT NULL,
                    customer_state           CHAR(2)      NOT NULL,
                    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
                )"""),
            ("geolocation", """
                CREATE TABLE IF NOT EXISTS olist.geolocation (
                    geolocation_zip_code_prefix VARCHAR(8)     NOT NULL,
                    geolocation_lat             DECIMAL(10, 8) NOT NULL,
                    geolocation_lng             DECIMAL(11, 8) NOT NULL,
                    geolocation_city            VARCHAR(100)   NOT NULL,
                    geolocation_state           CHAR(2)        NOT NULL
                )"""),
            ("sellers", """
                CREATE TABLE IF NOT EXISTS olist.sellers (
                    seller_id              VARCHAR(50)  NOT NULL,
                    seller_zip_code_prefix VARCHAR(8)   NOT NULL,
                    seller_city            VARCHAR(100) NOT NULL,
                    seller_state           CHAR(2)      NOT NULL,
                    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
                )"""),
            ("products", """
                CREATE TABLE IF NOT EXISTS olist.products (
                    product_id                 VARCHAR(50)    NOT NULL,
                    product_category_name      VARCHAR(100),
                    product_name_lenght        INTEGER,
                    product_description_lenght INTEGER,
                    product_photos_qty         INTEGER,
                    product_weight_g           DECIMAL(10, 2),
                    product_length_cm          DECIMAL(10, 2),
                    product_height_cm          DECIMAL(10, 2),
                    product_width_cm           DECIMAL(10, 2),
                    CONSTRAINT pk_products PRIMARY KEY (product_id)
                )"""),
            ("orders", """
                CREATE TABLE IF NOT EXISTS olist.orders (
                    order_id                      VARCHAR(50) NOT NULL,
                    customer_id                   VARCHAR(50) NOT NULL,
                    order_status                  VARCHAR(20) NOT NULL,
                    order_purchase_timestamp      TIMESTAMP,
                    order_approved_at             TIMESTAMP,
                    order_delivered_carrier_date  TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP,
                    CONSTRAINT pk_orders PRIMARY KEY (order_id),
                    CONSTRAINT fk_orders_customer
                        FOREIGN KEY (customer_id) REFERENCES olist.customers(customer_id)
                )"""),
            ("order_items", """
                CREATE TABLE IF NOT EXISTS olist.order_items (
                    order_id            VARCHAR(50)    NOT NULL,
                    order_item_id       INTEGER        NOT NULL,
                    product_id          VARCHAR(50)    NOT NULL,
                    seller_id           VARCHAR(50)    NOT NULL,
                    shipping_limit_date TIMESTAMP,
                    price               DECIMAL(10, 2) NOT NULL,
                    freight_value       DECIMAL(10, 2) NOT NULL,
                    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id),
                    CONSTRAINT fk_order_items_order
                        FOREIGN KEY (order_id) REFERENCES olist.orders(order_id) ON DELETE CASCADE,
                    CONSTRAINT fk_order_items_product
                        FOREIGN KEY (product_id) REFERENCES olist.products(product_id),
                    CONSTRAINT fk_order_items_seller
                        FOREIGN KEY (seller_id) REFERENCES olist.sellers(seller_id)
                )"""),
            ("order_payments", """
                CREATE TABLE IF NOT EXISTS olist.order_payments (
                    order_id             VARCHAR(50)    NOT NULL,
                    payment_sequential   INTEGER        NOT NULL,
                    payment_type         VARCHAR(30)    NOT NULL,
                    payment_installments INTEGER        NOT NULL DEFAULT 1,
                    payment_value        DECIMAL(10, 2) NOT NULL,
                    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential),
                    CONSTRAINT fk_payments_order
                        FOREIGN KEY (order_id) REFERENCES olist.orders(order_id) ON DELETE CASCADE
                )"""),
            ("order_reviews", """
                CREATE TABLE IF NOT EXISTS olist.order_reviews (
                    review_id               VARCHAR(50) NOT NULL,
                    order_id                VARCHAR(50) NOT NULL,
                    review_score            SMALLINT    NOT NULL,
                    review_comment_title    VARCHAR(255),
                    review_comment_message  TEXT,
                    review_creation_date    TIMESTAMP,
                    review_answer_timestamp TIMESTAMP,
                    CONSTRAINT pk_order_reviews PRIMARY KEY (review_id),
                    CONSTRAINT fk_reviews_order
                        FOREIGN KEY (order_id) REFERENCES olist.orders(order_id) ON DELETE CASCADE,
                    CONSTRAINT chk_review_score CHECK (review_score BETWEEN 1 AND 5)
                )"""),
        ]

        for table_name, sql in tables_sql:
            exec_single(conn, sql, f"CREATE TABLE {table_name}")
            logger.info(f"   ✅ Tabla olist.{table_name} lista")

        # Índices clave
        logger.info("   Creando índices...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON olist.orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON olist.orders(order_status)",
            "CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp ON olist.orders(order_purchase_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_score ON olist.order_reviews(review_score)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON olist.order_items(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_seller_id ON olist.order_items(seller_id)",
            "CREATE INDEX IF NOT EXISTS idx_geolocation_zip ON olist.geolocation(geolocation_zip_code_prefix)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON olist.products(product_category_name)",
        ]
        for idx_sql in indexes:
            exec_single(conn, idx_sql, "INDEX")

        conn.close()

    except Exception as e:
        logger.error(f"   ❌ Error creando esquema: {e}")
        return False

    logger.info("✅ Paso 3 completado: 9 tablas + índices creados en schema olist")
    return True


def step_4_load_data() -> bool:
    """Paso 4: Cargar los CSV en las tablas PostgreSQL."""
    logger.info("\n" + "━" * 60)
    logger.info("  PASO 4/5: CARGA DE DATOS (CSV → POSTGRESQL)")
    logger.info("━" * 60)

    from scripts.load_data import run_full_load
    results = run_full_load(truncate_first=True)

    total = sum(v for v in results.values() if isinstance(v, int))
    if total == 0:
        logger.error("❌ No se cargó ningún registro")
        return False

    logger.info(f"✅ Paso 4 completado: {total:,} registros cargados en total")
    return True


def step_5_verify() -> bool:
    """Paso 5: Verificación post-carga."""
    logger.info("\n" + "━" * 60)
    logger.info("  PASO 5/5: VERIFICACIÓN POST-CARGA")
    logger.info("━" * 60)

    from scripts.verify_load import run_verification
    success = run_verification()

    if success:
        logger.info("✅ Paso 5 completado: Verificación exitosa")
    else:
        logger.warning("⚠️  Paso 5: Verificación con advertencias")

    return True  # No bloquear el pipeline por advertencias


def main():
    """Punto de entrada principal del pipeline Parte 1."""
    t_inicio = time.time()

    print("\n" + "═" * 60)
    print("  PIPELINE PARTE 1 — FUENTE DE DATOS REAL")
    print("  Dataset: Olist Brazilian E-Commerce")
    print("  Base de datos: PostgreSQL (olist_db)")
    print("  Proyecto Final — Grupo 6 — LEAD University")
    print("═" * 60)

    steps = [
        ("Test de conexión",          step_1_test_connection),
        ("Verificación del dataset",  step_2_verify_dataset),
        ("Creación del esquema",      step_3_create_schema),
        ("Carga de datos",            step_4_load_data),
        ("Verificación post-carga",   step_5_verify),
    ]

    for i, (nombre, step_fn) in enumerate(steps, 1):
        try:
            success = step_fn()
            if not success:
                logger.error(f"\n❌ Pipeline detenido en el paso {i}: {nombre}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"\n❌ Error inesperado en paso {i} ({nombre}): {e}", exc_info=True)
            sys.exit(1)

    elapsed = time.time() - t_inicio

    print("\n" + "═" * 60)
    print(f"  ✅ PARTE 1 COMPLETADA EXITOSAMENTE")
    print(f"  Tiempo total: {elapsed:.1f} segundos")
    print(f"  Log guardado en: {log_filename}")
    print("═" * 60)
    print("\n  La base de datos PostgreSQL está lista.")
    print("  La Parte 2 (ETL) puede extraer datos de olist_db.")
    print()


if __name__ == "__main__":
    main()
