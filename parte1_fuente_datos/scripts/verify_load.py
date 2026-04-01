"""
verify_load.py
==============
Script de verificación post-carga: valida que los datos están correctamente
almacenados en PostgreSQL. Genera un reporte de auditoría.

Genera evidencia de la carga exitosa (requerida por el proyecto).

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Agregar directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.db_connection import get_database_connection

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist.verify")

# Directorio de reportes
REPORTS_DIR = Path(__file__).parent.parent / "data" / "reports"


def verify_table_counts(engine) -> pd.DataFrame:
    """Verifica cantidad de registros en cada tabla."""
    query = """
    SELECT 'olist.customers'                      AS tabla, COUNT(*) AS registros FROM olist.customers
    UNION ALL
    SELECT 'olist.geolocation',                              COUNT(*) FROM olist.geolocation
    UNION ALL
    SELECT 'olist.sellers',                                  COUNT(*) FROM olist.sellers
    UNION ALL
    SELECT 'olist.products',                                 COUNT(*) FROM olist.products
    UNION ALL
    SELECT 'olist.product_category_name_translation',        COUNT(*) FROM olist.product_category_name_translation
    UNION ALL
    SELECT 'olist.orders',                                   COUNT(*) FROM olist.orders
    UNION ALL
    SELECT 'olist.order_items',                              COUNT(*) FROM olist.order_items
    UNION ALL
    SELECT 'olist.order_payments',                           COUNT(*) FROM olist.order_payments
    UNION ALL
    SELECT 'olist.order_reviews',                            COUNT(*) FROM olist.order_reviews
    ORDER BY registros DESC
    """
    return pd.read_sql(query, engine)


def verify_review_distribution(engine) -> pd.DataFrame:
    """Distribución del review_score (variable target del modelo IA)."""
    query = """
    SELECT
        review_score,
        COUNT(*) AS cantidad,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS porcentaje
    FROM olist.order_reviews
    GROUP BY review_score
    ORDER BY review_score
    """
    return pd.read_sql(query, engine)


def verify_data_completeness(engine) -> pd.DataFrame:
    """Verifica nulos en columnas clave."""
    query = """
    SELECT
        'orders.customer_id nulos'           AS metrica, COUNT(*) AS valor
        FROM olist.orders WHERE customer_id IS NULL
    UNION ALL
    SELECT
        'orders.order_status nulos',          COUNT(*)
        FROM olist.orders WHERE order_status IS NULL
    UNION ALL
    SELECT
        'order_reviews.review_score nulos',   COUNT(*)
        FROM olist.order_reviews WHERE review_score IS NULL
    UNION ALL
    SELECT
        'order_items.price nulos',            COUNT(*)
        FROM olist.order_items WHERE price IS NULL
    UNION ALL
    SELECT
        'customers.customer_state nulos',     COUNT(*)
        FROM olist.customers WHERE customer_state IS NULL
    """
    return pd.read_sql(query, engine)


def verify_referential_integrity(engine) -> pd.DataFrame:
    """Verifica integridad referencial (registros huérfanos)."""
    query = """
    SELECT
        'orders sin customer'          AS verificacion,
        COUNT(*) AS registros_huerfanos
    FROM olist.orders o
    LEFT JOIN olist.customers c ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL

    UNION ALL

    SELECT
        'order_items sin order',
        COUNT(*)
    FROM olist.order_items oi
    LEFT JOIN olist.orders o ON oi.order_id = o.order_id
    WHERE o.order_id IS NULL

    UNION ALL

    SELECT
        'reviews sin order',
        COUNT(*)
    FROM olist.order_reviews r
    LEFT JOIN olist.orders o ON r.order_id = o.order_id
    WHERE o.order_id IS NULL

    UNION ALL

    SELECT
        'payments sin order',
        COUNT(*)
    FROM olist.order_payments p
    LEFT JOIN olist.orders o ON p.order_id = o.order_id
    WHERE o.order_id IS NULL
    """
    return pd.read_sql(query, engine)


def verify_date_range(engine) -> pd.DataFrame:
    """Verifica el rango temporal del dataset."""
    query = """
    SELECT
        MIN(order_purchase_timestamp)::DATE                                    AS primera_orden,
        MAX(order_purchase_timestamp)::DATE                                    AS ultima_orden,
        COUNT(DISTINCT DATE_TRUNC('month', order_purchase_timestamp))          AS meses_cubiertos,
        COUNT(*)                                                               AS total_ordenes
    FROM olist.orders
    WHERE order_purchase_timestamp IS NOT NULL
    """
    return pd.read_sql(query, engine)


def run_verification() -> bool:
    """
    Ejecuta todas las verificaciones y genera reporte de auditoría.

    Returns:
        True si todas las verificaciones pasan.
    """
    logger.info("\n" + "=" * 60)
    logger.info("  VERIFICACIÓN POST-CARGA — OLIST DB")
    logger.info("=" * 60)

    db = get_database_connection()
    engine = db.get_engine()

    all_passed = True
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------
    # 1. Conteo de registros
    # -------------------------------------------------------
    logger.info("\n📊 1. CONTEO DE REGISTROS POR TABLA:")
    df_counts = verify_table_counts(engine)
    print(df_counts.to_string(index=False))

    # Verificar umbrales mínimos esperados del dataset Olist
    expected_min = {
        "olist.customers":   90_000,
        "olist.orders":      90_000,
        "olist.order_reviews": 90_000,
    }

    for tabla, minimo in expected_min.items():
        actual = df_counts[df_counts["tabla"] == tabla]["registros"].values
        if len(actual) > 0 and actual[0] >= minimo:
            logger.info(f"   ✅ {tabla}: {actual[0]:,} (>= {minimo:,})")
        else:
            logger.warning(f"   ⚠️  {tabla}: bajo el mínimo esperado ({minimo:,})")
            all_passed = False

    # -------------------------------------------------------
    # 2. Distribución de review_score
    # -------------------------------------------------------
    logger.info("\n⭐ 2. DISTRIBUCIÓN DE REVIEW_SCORE (target IA):")
    df_reviews = verify_review_distribution(engine)
    print(df_reviews.to_string(index=False))

    # -------------------------------------------------------
    # 3. Completitud de datos
    # -------------------------------------------------------
    logger.info("\n🔍 3. VALORES NULOS EN COLUMNAS CLAVE:")
    df_nulls = verify_data_completeness(engine)
    print(df_nulls.to_string(index=False))

    # -------------------------------------------------------
    # 4. Integridad referencial
    # -------------------------------------------------------
    logger.info("\n🔗 4. INTEGRIDAD REFERENCIAL:")
    df_integrity = verify_referential_integrity(engine)
    print(df_integrity.to_string(index=False))

    if df_integrity["registros_huerfanos"].sum() > 0:
        logger.warning("   ⚠️  Existen registros huérfanos (puede ser normal en Olist)")
    else:
        logger.info("   ✅ Integridad referencial correcta")

    # -------------------------------------------------------
    # 5. Rango temporal
    # -------------------------------------------------------
    logger.info("\n📅 5. RANGO TEMPORAL DEL DATASET:")
    df_dates = verify_date_range(engine)
    print(df_dates.to_string(index=False))

    # -------------------------------------------------------
    # Guardar reporte como CSV
    # -------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"verificacion_carga_{timestamp}.csv"

    pd.concat([
        df_counts.assign(seccion="1_conteo_registros"),
        df_reviews.assign(seccion="2_review_distribution"),
        df_nulls.assign(seccion="3_nulos"),
        df_integrity.assign(seccion="4_integridad"),
    ]).to_csv(report_path, index=False)

    logger.info(f"\n📄 Reporte guardado en: {report_path}")
    logger.info("\n" + "=" * 60)

    if all_passed:
        logger.info("  ✅ VERIFICACIÓN EXITOSA — Base de datos lista para ETL")
    else:
        logger.warning("  ⚠️  Verificación con advertencias — revisar logs")

    logger.info("=" * 60)

    db.close()
    return all_passed


if __name__ == "__main__":
    success = run_verification()
    sys.exit(0 if success else 1)
