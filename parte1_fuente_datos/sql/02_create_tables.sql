-- ============================================================
-- 02_create_tables.sql
-- Definición del esquema relacional completo de Olist en PostgreSQL
--
-- Proyecto Final — Administración de Datos — LEAD University
-- Grupo 6 | Parte 1: Fuente de Datos Real
-- Dataset: Olist Brazilian E-Commerce (~115,000 órdenes reales)
-- Autor: Esteban Gutiérrez Saborío
--
-- INSTRUCCIONES:
--   psql -U postgres -d olist_db -f sql/02_create_tables.sql
-- ============================================================

-- Conectar a la base olist_db antes de ejecutar
\connect olist_db

-- ============================================================
-- SCHEMA: olist
-- Separamos en un schema propio para mantener la BD organizada
-- y separar datos crudos de capas procesadas (ETL)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS olist
    AUTHORIZATION postgres;

COMMENT ON SCHEMA olist IS
    'Schema de datos crudos — Olist Brazilian E-Commerce.
     Contiene las 9 tablas originales del dataset.';

-- Hacer que olist sea el schema por defecto en esta sesión
SET search_path TO olist, public;

-- ============================================================
-- TABLA 1: product_category_name_translation
-- Catálogo de traducción de categorías de productos (PT → EN)
-- Sin dependencias — se crea primero
-- ============================================================
DROP TABLE IF EXISTS olist.product_category_name_translation CASCADE;

CREATE TABLE olist.product_category_name_translation (
    product_category_name         VARCHAR(100) NOT NULL,
    product_category_name_english VARCHAR(100) NOT NULL,

    CONSTRAINT pk_product_category PRIMARY KEY (product_category_name)
);

COMMENT ON TABLE olist.product_category_name_translation IS
    'Traducción de categorías de productos del portugués al inglés.';

-- ============================================================
-- TABLA 2: customers
-- Información de los clientes que realizaron órdenes
-- ============================================================
DROP TABLE IF EXISTS olist.customers CASCADE;

CREATE TABLE olist.customers (
    customer_id              VARCHAR(50)  NOT NULL,
    customer_unique_id       VARCHAR(50)  NOT NULL,
    customer_zip_code_prefix VARCHAR(8)   NOT NULL,
    customer_city            VARCHAR(100) NOT NULL,
    customer_state           CHAR(2)      NOT NULL,

    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

COMMENT ON TABLE olist.customers IS
    'Clientes del marketplace Olist. customer_unique_id identifica al cliente real
     (un cliente puede tener múltiples customer_id por compra).';
COMMENT ON COLUMN olist.customers.customer_unique_id IS
    'ID único del cliente real (anonimizado). Puede tener múltiples pedidos.';
COMMENT ON COLUMN olist.customers.customer_zip_code_prefix IS
    'Primeros 5 dígitos del código postal del cliente.';

-- ============================================================
-- TABLA 3: geolocation
-- Coordenadas geográficas por código postal de Brasil
-- ============================================================
DROP TABLE IF EXISTS olist.geolocation CASCADE;

CREATE TABLE olist.geolocation (
    geolocation_zip_code_prefix VARCHAR(8)      NOT NULL,
    geolocation_lat             DECIMAL(10, 8)  NOT NULL,
    geolocation_lng             DECIMAL(11, 8)  NOT NULL,
    geolocation_city            VARCHAR(100)    NOT NULL,
    geolocation_state           CHAR(2)         NOT NULL

    -- Sin PK: hay múltiples coordenadas por zip code (promedio en ETL)
);

COMMENT ON TABLE olist.geolocation IS
    'Datos de geolocalización por código postal. Una sola ZIP puede tener
     múltiples coordenadas — el ETL calculará el centroide.';

-- ============================================================
-- TABLA 4: sellers
-- Vendedores que ofrecen productos en el marketplace Olist
-- ============================================================
DROP TABLE IF EXISTS olist.sellers CASCADE;

CREATE TABLE olist.sellers (
    seller_id              VARCHAR(50)  NOT NULL,
    seller_zip_code_prefix VARCHAR(8)   NOT NULL,
    seller_city            VARCHAR(100) NOT NULL,
    seller_state           CHAR(2)      NOT NULL,

    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);

COMMENT ON TABLE olist.sellers IS
    'Vendedores registrados en el marketplace Olist.';

-- ============================================================
-- TABLA 5: products
-- Catálogo de productos del marketplace
-- ============================================================
DROP TABLE IF EXISTS olist.products CASCADE;

CREATE TABLE olist.products (
    product_id                 VARCHAR(50)  NOT NULL,
    product_category_name      VARCHAR(100),
    product_name_lenght        INTEGER,      -- typo original del dataset
    product_description_lenght INTEGER,      -- typo original del dataset
    product_photos_qty         INTEGER,
    product_weight_g           DECIMAL(10, 2),
    product_length_cm          DECIMAL(10, 2),
    product_height_cm          DECIMAL(10, 2),
    product_width_cm           DECIMAL(10, 2),

    CONSTRAINT pk_products PRIMARY KEY (product_id),

    -- Relación con la tabla de traducciones (puede ser NULL si no existe traducción)
    CONSTRAINT fk_products_category
        FOREIGN KEY (product_category_name)
        REFERENCES olist.product_category_name_translation (product_category_name)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

COMMENT ON TABLE olist.products IS
    'Catálogo de productos. Dimensiones en cm, peso en gramos.';
COMMENT ON COLUMN olist.products.product_name_lenght IS
    'Cantidad de caracteres en el nombre del producto (typo original del dataset).';

-- ============================================================
-- TABLA 6: orders
-- Órdenes de compra del marketplace — tabla central del modelo
-- ============================================================
DROP TABLE IF EXISTS olist.orders CASCADE;

CREATE TABLE olist.orders (
    order_id                        VARCHAR(50) NOT NULL,
    customer_id                     VARCHAR(50) NOT NULL,
    order_status                    VARCHAR(20) NOT NULL,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,

    CONSTRAINT pk_orders PRIMARY KEY (order_id),

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES olist.customers (customer_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    -- Validar que el status sea un valor conocido del dataset
    CONSTRAINT chk_order_status CHECK (
        order_status IN (
            'created', 'approved', 'processing', 'invoiced',
            'shipped', 'delivered', 'unavailable', 'canceled'
        )
    )
);

COMMENT ON TABLE olist.orders IS
    'Órdenes de compra — tabla central del modelo estrella.
     Contiene los timestamps de cada etapa del ciclo de vida de la orden.';
COMMENT ON COLUMN olist.orders.order_status IS
    'Estado de la orden: created → approved → processing → invoiced → shipped → delivered';

-- ============================================================
-- TABLA 7: order_items
-- Ítems individuales dentro de cada orden
-- ============================================================
DROP TABLE IF EXISTS olist.order_items CASCADE;

CREATE TABLE olist.order_items (
    order_id            VARCHAR(50)    NOT NULL,
    order_item_id       INTEGER        NOT NULL,   -- Secuencial dentro de la orden
    product_id          VARCHAR(50)    NOT NULL,
    seller_id           VARCHAR(50)    NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(10, 2) NOT NULL,
    freight_value       DECIMAL(10, 2) NOT NULL,

    -- PK compuesta: una orden puede tener múltiples ítems
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id),

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES olist.orders (order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES olist.products (product_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_order_items_seller
        FOREIGN KEY (seller_id)
        REFERENCES olist.sellers (seller_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    -- Validaciones de integridad
    CONSTRAINT chk_price_positive      CHECK (price >= 0),
    CONSTRAINT chk_freight_nonnegative CHECK (freight_value >= 0)
);

COMMENT ON TABLE olist.order_items IS
    'Ítems individuales dentro de cada orden. Una orden puede contener
     múltiples ítems de diferentes vendedores.';

-- ============================================================
-- TABLA 8: order_payments
-- Métodos y valores de pago por orden
-- ============================================================
DROP TABLE IF EXISTS olist.order_payments CASCADE;

CREATE TABLE olist.order_payments (
    order_id             VARCHAR(50)    NOT NULL,
    payment_sequential   INTEGER        NOT NULL,  -- Secuencial si hay múltiples pagos
    payment_type         VARCHAR(30)    NOT NULL,
    payment_installments INTEGER        NOT NULL DEFAULT 1,
    payment_value        DECIMAL(10, 2) NOT NULL,

    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential),

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES olist.orders (order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT chk_payment_type CHECK (
        payment_type IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined')
    ),

    CONSTRAINT chk_payment_value_positive CHECK (payment_value >= 0),
    CONSTRAINT chk_installments_positive  CHECK (payment_installments >= 1)
);

COMMENT ON TABLE olist.order_payments IS
    'Información de pago de cada orden. Una orden puede tener múltiples
     métodos de pago (payment_sequential > 1).';

-- ============================================================
-- TABLA 9: order_reviews
-- Reseñas y calificaciones de clientes (VARIABLE TARGET del modelo IA)
-- ============================================================
DROP TABLE IF EXISTS olist.order_reviews CASCADE;

CREATE TABLE olist.order_reviews (
    review_id                VARCHAR(50)  NOT NULL,
    order_id                 VARCHAR(50)  NOT NULL,
    review_score             SMALLINT     NOT NULL,
    review_comment_title     VARCHAR(255),
    review_comment_message   TEXT,
    review_creation_date     TIMESTAMP,
    review_answer_timestamp  TIMESTAMP,

    CONSTRAINT pk_order_reviews PRIMARY KEY (review_id),

    CONSTRAINT fk_reviews_order
        FOREIGN KEY (order_id)
        REFERENCES olist.orders (order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    -- El score debe estar entre 1 y 5 estrellas
    CONSTRAINT chk_review_score CHECK (review_score BETWEEN 1 AND 5)
);

COMMENT ON TABLE olist.order_reviews IS
    'Reseñas de clientes. review_score (1-5) es la VARIABLE TARGET del modelo
     de IA para predicción de satisfacción del cliente.';
COMMENT ON COLUMN olist.order_reviews.review_score IS
    'Calificación del cliente (1=muy malo, 5=excelente). Variable objetivo del modelo de IA.';

-- ============================================================
-- VERIFICACIÓN FINAL
-- ============================================================
\echo ''
\echo '=============================================='
\echo '✅ Esquema olist creado con 9 tablas reales:'
\echo '   1. product_category_name_translation'
\echo '   2. customers'
\echo '   3. geolocation'
\echo '   4. sellers'
\echo '   5. products'
\echo '   6. orders          ← Tabla central'
\echo '   7. order_items'
\echo '   8. order_payments'
\echo '   9. order_reviews   ← Variable target IA'
\echo '=============================================='
\echo '   Próximo paso: ejecutar sql/03_create_indexes.sql'
\echo ''
