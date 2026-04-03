"""
verify_load.py
==============
Script de verificación post-carga: valida que los datos están correctamente
almacenados en PostgreSQL. Genera un reporte de auditoría.

Genera evidencia de la carga exitosa (requerida por el proyecto).

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
Modificado por: Ariana Víquez S.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Agregar directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.db_connection import get_database_connection

# ====
# LOGGING
# ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("books.verify")

# Directorio de reportes
REPORTS_DIR = Path(__file__).parent.parent / "data" / "reports"


def verify_table_counts(engine) -> pd.DataFrame:
    """Verifica cantidad de registros en cada tabla."""
    query = """
    SELECT 'books.books_data'   AS tabla, COUNT(*) AS registros FROM books.books_data
    UNION ALL
    SELECT 'books.books_rating', COUNT(*) FROM books.books_rating
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
    FROM books.books_rating
    WHERE review_score IS NOT NULL
    GROUP BY review_score
    ORDER BY review_score
    """
    return pd.read_sql(query, engine)


def verify_data_completeness(engine) -> pd.DataFrame:
    """Verifica nulos en columnas clave."""
    query = """
    SELECT 'books_rating.review_score nulos'  AS metrica, COUNT(*) AS valor
        FROM books.books_rating WHERE review_score IS NULL
    UNION ALL
    SELECT 'books_rating.user_id nulos',       COUNT(*)
        FROM books.books_rating WHERE user_id IS NULL
    UNION ALL
    SELECT 'books_rating.title nulos',         COUNT(*)
        FROM books.books_rating WHERE title IS NULL
    UNION ALL
    SELECT 'books_rating.review_time nulos',   COUNT(*)
        FROM books.books_rating WHERE review_time IS NULL
    UNION ALL
    SELECT 'books_data.title nulos',           COUNT(*)
        FROM books.books_data WHERE title IS NULL
    UNION ALL
    SELECT 'books_data.authors nulos',         COUNT(*)
        FROM books.books_data WHERE authors IS NULL
    """
    return pd.read_sql(query, engine)


def verify_score_range(engine) -> pd.DataFrame:
    """Verifica que review_score esté en el rango válido (1-5)."""
    query = """
    SELECT
        MIN(review_score)                                   AS score_minimo,
        MAX(review_score)                                   AS score_maximo,
        ROUND(AVG(review_score), 2)                         AS score_promedio,
        COUNT(*) FILTER (WHERE review_score NOT BETWEEN 1 AND 5) AS fuera_de_rango
    FROM books.books_rating
    WHERE review_score IS NOT NULL
    """
    return pd.read_sql(query, engine)


def verify_date_range(engine) -> pd.DataFrame:
    """Verifica el rango temporal del dataset usando review_time (Unix timestamp)."""
    query = """
    SELECT
        MIN(TO_TIMESTAMP(review_time))::DATE    AS primera_resena,
        MAX(TO_TIMESTAMP(review_time))::DATE    AS ultima_resena,
        COUNT(*)                                AS total_resenas
    FROM books.books_rating
    WHERE review_time IS NOT NULL
    """
    return pd.read_sql(query, engine)


def run_verification() -> bool:
    """
    Ejecuta todas las verificaciones y genera reporte de auditoría.

    Returns:
        True si todas las verificaciones pasan.
    """
    logger.info("\n" + "=" * 60)
    logger.info("  VERIFICACIÓN POST-CARGA — AMAZON BOOKS DB")
    logger.info("=" * 60)

    db = get_database_connection()
    engine = db.get_engine()

    all_passed = True
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # ----
    # 1. Conteo de registros
    # ----
    logger.info("\n📊 1. CONTEO DE REGISTROS POR TABLA:")
    df_counts = verify_table_counts(engine)
    print(df_counts.to_string(index=False))

    # Verificar umbrales mínimos esperados
    expected_min = {
        "books.books_rating": 100_000,
        "books.books_data":   10_000,
    }

    for tabla, minimo in expected_min.items():
        actual = df_counts[df_counts["tabla"] == tabla]["registros"].values
        if len(actual) > 0 and actual[0] >= minimo:
            logger.info(f"   ✅ {tabla}: {actual[0]:,} (>= {minimo:,})")
        else:
            val = actual[0] if len(actual) > 0 else 0
            logger.warning(f"   ⚠️  {tabla}: {val:,} — bajo el mínimo esperado ({minimo:,})")
            all_passed = False

    # ----
    # 2. Distribución de review_score
    # ----
    logger.info("\n⭐ 2. DISTRIBUCIÓN DE REVIEW_SCORE (target IA):")
    df_reviews = verify_review_distribution(engine)
    print(df_reviews.to_string(index=False))

    # ----
    # 3. Completitud de datos
    # ----
    logger.info("\n🔍 3. VALORES NULOS EN COLUMNAS CLAVE:")
    df_nulls = verify_data_completeness(engine)
    print(df_nulls.to_string(index=False))

    # ----
    # 4. Rango válido de scores
    # ----
    logger.info("\n🎯 4. RANGO Y PROMEDIO DE REVIEW_SCORE:")
    df_scores = verify_score_range(engine)
    print(df_scores.to_string(index=False))

    fuera_rango = df_scores["fuera_de_rango"].values[0] if len(df_scores) > 0 else 0
    if fuera_rango > 0:
        logger.warning(f"   ⚠️  {fuera_rango:,} registros con score fuera del rango 1-5")
    else:
        logger.info("   ✅ Todos los scores están en rango válido (1-5)")

    # ----
    # 5. Rango temporal
    # ----
    logger.info("\n📅 5. RANGO TEMPORAL DEL DATASET:")
    df_dates = verify_date_range(engine)
    print(df_dates.to_string(index=False))

    # ----
    # Guardar reporte como CSV
    # ----
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"verificacion_carga_{timestamp}.csv"

    pd.concat([
        df_counts.assign(seccion="1_conteo_registros"),
        df_reviews.assign(seccion="2_review_distribution"),
        df_nulls.assign(seccion="3_nulos"),
        df_scores.assign(seccion="4_score_range"),
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