-- ============================================================
-- 04_verify_schema.sql
-- Verificación del esquema: tablas, columnas, FK y conteo de registros
--
-- Proyecto Final — Administración de Datos — LEAD University
-- Grupo 6 | Parte 1: Fuente de Datos Real
-- Autor: Esteban Gutiérrez Saborío
--
-- INSTRUCCIONES (ejecutar DESPUÉS de cargar los datos):
--   psql -U postgres -d olist_db -f sql/04_verify_schema.sql
-- ============================================================

\connect olist_db
SET search_path TO olist, public;

\echo '=============================================='
\echo '  VERIFICACIÓN DEL ESQUEMA — OLIST DB'
\echo '=============================================='

-- ============================================================
-- 1. Listar todas las tablas del schema olist
-- ============================================================
\echo ''
\echo '1. TABLAS EN EL SCHEMA olist:'
SELECT
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'olist'
ORDER BY table_name;

-- ============================================================
-- 2. Conteo de registros por tabla (evidencia de carga exitosa)
-- ============================================================
\echo ''
\echo '2. CONTEO DE REGISTROS POR TABLA:'
SELECT 'customers'                      AS tabla, COUNT(*) AS registros FROM olist.customers
UNION ALL
SELECT 'geolocation',                              COUNT(*) FROM olist.geolocation
UNION ALL
SELECT 'sellers',                                  COUNT(*) FROM olist.sellers
UNION ALL
SELECT 'products',                                 COUNT(*) FROM olist.products
UNION ALL
SELECT 'product_category_name_translation',        COUNT(*) FROM olist.product_category_name_translation
UNION ALL
SELECT 'orders',                                   COUNT(*) FROM olist.orders
UNION ALL
SELECT 'order_items',                              COUNT(*) FROM olist.order_items
UNION ALL
SELECT 'order_payments',                           COUNT(*) FROM olist.order_payments
UNION ALL
SELECT 'order_reviews',                            COUNT(*) FROM olist.order_reviews
ORDER BY registros DESC;

-- ============================================================
-- 3. Verificar integridad referencial (registros huérfanos)
-- ============================================================
\echo ''
\echo '3. VERIFICACIÓN DE INTEGRIDAD REFERENCIAL:'

-- Órdenes sin cliente
SELECT
    'Órdenes sin cliente (huérfanas)' AS verificacion,
    COUNT(*) AS cantidad
FROM olist.orders o
LEFT JOIN olist.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Order items sin orden
SELECT
    'Order items sin orden (huérfanos)' AS verificacion,
    COUNT(*) AS cantidad
FROM olist.order_items oi
LEFT JOIN olist.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Reviews sin orden
SELECT
    'Reviews sin orden (huérfanas)' AS verificacion,
    COUNT(*) AS cantidad
FROM olist.order_reviews r
LEFT JOIN olist.orders o ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- ============================================================
-- 4. Distribución de review_score (variable target IA)
-- ============================================================
\echo ''
\echo '4. DISTRIBUCIÓN DEL REVIEW SCORE (variable target del modelo IA):'
SELECT
    review_score,
    COUNT(*)                                                         AS cantidad,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)             AS porcentaje
FROM olist.order_reviews
GROUP BY review_score
ORDER BY review_score;

-- ============================================================
-- 5. Distribución de estados de órdenes
-- ============================================================
\echo ''
\echo '5. DISTRIBUCIÓN DE ESTADOS DE ÓRDENES:'
SELECT
    order_status,
    COUNT(*) AS cantidad,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM olist.orders
GROUP BY order_status
ORDER BY cantidad DESC;

-- ============================================================
-- 6. Rango temporal de los datos
-- ============================================================
\echo ''
\echo '6. RANGO TEMPORAL DEL DATASET:'
SELECT
    MIN(order_purchase_timestamp) AS primera_orden,
    MAX(order_purchase_timestamp) AS ultima_orden,
    COUNT(DISTINCT DATE_TRUNC('month', order_purchase_timestamp)) AS meses_cubiertos
FROM olist.orders
WHERE order_purchase_timestamp IS NOT NULL;

\echo ''
\echo '✅ Verificación completada. Base de datos lista para el ETL (Parte 2).'
\echo ''
