-- ============================================================
-- 03_create_indexes.sql
-- Índices para optimizar las consultas del pipeline ETL
--
-- Proyecto Final — Administración de Datos — LEAD University
-- Grupo 6 | Parte 1: Fuente de Datos Real
-- Autor: Esteban Gutiérrez Saborío
--
-- INSTRUCCIONES:
--   psql -U postgres -d olist_db -f sql/03_create_indexes.sql
-- ============================================================

\connect olist_db
SET search_path TO olist, public;

\echo '⏳ Creando índices para optimización de consultas...'

-- ============================================================
-- ÍNDICES EN: customers
-- ============================================================
-- Búsqueda de cliente único (para deduplicación en ETL)
CREATE INDEX IF NOT EXISTS idx_customers_unique_id
    ON olist.customers (customer_unique_id);

-- Búsqueda geográfica por estado/ciudad
CREATE INDEX IF NOT EXISTS idx_customers_state
    ON olist.customers (customer_state);

CREATE INDEX IF NOT EXISTS idx_customers_city_state
    ON olist.customers (customer_city, customer_state);

-- ============================================================
-- ÍNDICES EN: orders
-- ============================================================
-- FK más consultada en joins
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON olist.orders (customer_id);

-- Filtros por estado de orden (muy frecuente en ETL)
CREATE INDEX IF NOT EXISTS idx_orders_status
    ON olist.orders (order_status);

-- Análisis temporal (extracción por rango de fechas)
CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp
    ON olist.orders (order_purchase_timestamp);

CREATE INDEX IF NOT EXISTS idx_orders_delivered_date
    ON olist.orders (order_delivered_customer_date);

-- Índice compuesto: status + fecha (consultas más comunes del ETL)
CREATE INDEX IF NOT EXISTS idx_orders_status_date
    ON olist.orders (order_status, order_purchase_timestamp DESC);

-- ============================================================
-- ÍNDICES EN: order_items
-- ============================================================
-- FK de producto y vendedor (usados en joins del ETL)
CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON olist.order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_order_items_seller_id
    ON olist.order_items (seller_id);

-- Precio y flete (feature engineering)
CREATE INDEX IF NOT EXISTS idx_order_items_price
    ON olist.order_items (price);

-- ============================================================
-- ÍNDICES EN: order_payments
-- ============================================================
-- Tipo de pago (usado en feature engineering del ETL)
CREATE INDEX IF NOT EXISTS idx_payments_type
    ON olist.order_payments (payment_type);

CREATE INDEX IF NOT EXISTS idx_payments_installments
    ON olist.order_payments (payment_installments);

-- ============================================================
-- ÍNDICES EN: order_reviews
-- ============================================================
-- El score es la variable target — muy consultada
CREATE INDEX IF NOT EXISTS idx_reviews_score
    ON olist.order_reviews (review_score);

-- Fecha de creación de reseña (análisis temporal)
CREATE INDEX IF NOT EXISTS idx_reviews_creation_date
    ON olist.order_reviews (review_creation_date);

-- ============================================================
-- ÍNDICES EN: products
-- ============================================================
-- Categoría (feature importante para el modelo de IA)
CREATE INDEX IF NOT EXISTS idx_products_category
    ON olist.products (product_category_name);

-- ============================================================
-- ÍNDICES EN: sellers
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_sellers_state
    ON olist.sellers (seller_state);

-- ============================================================
-- ÍNDICES EN: geolocation
-- ============================================================
-- Lookup por zip code (join con customers y sellers)
CREATE INDEX IF NOT EXISTS idx_geolocation_zip
    ON olist.geolocation (geolocation_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_geolocation_state
    ON olist.geolocation (geolocation_state);

-- ============================================================
-- VERIFICACIÓN
-- ============================================================
\echo ''
\echo '✅ Índices creados exitosamente.'
\echo ''

-- Consulta de verificación: listar todos los índices creados
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'olist'
ORDER BY tablename, indexname;
