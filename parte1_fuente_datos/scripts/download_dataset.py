"""
download_dataset.py
===================
Módulo de verificación del dataset Olist Brazilian E-Commerce.

El dataset Olist se descarga manualmente desde Kaggle y se coloca en
data/raw/. Este script verifica que todos los archivos estén presentes
y legibles antes de proceder con la carga a PostgreSQL.

Descarga manual (un solo paso):
  1. Ve a: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  2. Click en "Download" → descarga el .zip
  3. Extrae todos los CSV en: parte1_fuente_datos/data/raw/

Proyecto Final — Administración de Datos — LEAD University
Grupo 6 | Parte 1: Fuente de Datos Real
Autor: Esteban Gutiérrez Saborío
"""

import logging
import sys
from pathlib import Path

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist.dataset")

# ============================================================
# CONSTANTES
# ============================================================
DATA_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
KAGGLE_DATASET_URL = "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"

# Archivos esperados del dataset Olist (9 CSV)
EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]


def check_files_already_exist() -> bool:
    """
    Verifica que todos los archivos del dataset están en data/raw/.

    Returns:
        True si los 9 archivos existen y tienen contenido.
    """
    if not DATA_RAW_DIR.exists():
        return False

    existing = [f for f in EXPECTED_FILES if (DATA_RAW_DIR / f).exists()]
    if len(existing) == len(EXPECTED_FILES):
        logger.info(f"✅ Dataset completo: {len(existing)}/{len(EXPECTED_FILES)} archivos en data/raw/")
        return True

    logger.info(f"   Dataset incompleto: {len(existing)}/{len(EXPECTED_FILES)} archivos encontrados")
    return False


def download_dataset() -> bool:
    """
    Verifica que el dataset Olist está disponible en data/raw/.

    Si los archivos no están presentes, muestra las instrucciones de
    descarga manual desde Kaggle.

    Returns:
        True si todos los archivos del dataset están disponibles.
    """
    # Verificar si ya está descargado
    if check_files_already_exist():
        return True

    # Mostrar instrucciones detalladas para descarga manual
    logger.error(
        "❌ Dataset Olist no encontrado en data/raw/\n\n"
        "  ╔══════════════════════════════════════════════════════╗\n"
        "  ║         DESCARGA MANUAL DEL DATASET OLIST           ║\n"
        "  ╠══════════════════════════════════════════════════════╣\n"
        f"  ║  1. Ve a: {KAGGLE_DATASET_URL}\n"
        "  ║  2. Inicia sesión en Kaggle (cuenta gratuita)\n"
        "  ║  3. Click en el botón 'Download' (arriba a la derecha)\n"
        "  ║  4. Extrae el .zip descargado\n"
        "  ║  5. Copia los 9 CSV a la carpeta:\n"
        f"  ║     {DATA_RAW_DIR}\n"
        "  ╚══════════════════════════════════════════════════════╝\n"
    )
    return False


def verify_dataset() -> bool:
    """
    Verificación detallada: muestra tamaño de cada archivo.

    Returns:
        True si todos los archivos están presentes y legibles.
    """
    logger.info("\n📋 Verificando archivos del dataset Olist:")
    all_ok = True
    total_mb = 0.0

    for filename in EXPECTED_FILES:
        file_path = DATA_RAW_DIR / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            total_mb += size_mb
            logger.info(f"   ✓ {filename:<55} ({size_mb:.2f} MB)")
        else:
            logger.warning(f"   ✗ FALTA: {filename}")
            all_ok = False

    if all_ok:
        logger.info(f"\n✅ Todos los archivos presentes — Total: {total_mb:.1f} MB")
        logger.info(f"   Directorio: {DATA_RAW_DIR}")
    else:
        logger.warning("\n⚠️  Archivos faltantes. Ver instrucciones de descarga arriba.")

    return all_ok


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  VERIFICACIÓN DEL DATASET — OLIST BRAZILIAN E-COMMERCE")
    print("=" * 55)

    success = download_dataset()

    if success:
        verify_dataset()
        print("\n✅ Dataset listo. Ejecuta: python scripts/run_parte1.py")
    else:
        sys.exit(1)
