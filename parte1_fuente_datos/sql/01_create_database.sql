-- ============================================================
-- 01_create_database.sql
-- Creación de la base de datos PostgreSQL para el proyecto
--
-- Proyecto Final — Administración de Datos — LEAD University
-- Grupo 6 | Parte 1: Fuente de Datos Real
-- Dataset: Amazon Books Reviews
-- Autor: Esteban Gutiérrez Saborío
-- Modificado por: Ariana Víquez S.
-- ============================================================
--
-- INSTRUCCIONES:
-- Ejecutar este script conectado como superusuario (postgres):
--   psql -U postgres -f sql/01_create_database.sql
-- ============================================================

-- Terminar conexiones activas a la base si existe
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'amazon_books_db' AND pid <> pg_backend_pid();

-- Eliminar la base si ya existe (para re-creación limpia)
DROP DATABASE IF EXISTS amazon_books_db;

-- Crear la base de datos con encoding UTF8 para soportar caracteres especiales
CREATE DATABASE amazon_books_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0
    CONNECTION LIMIT = -1;

-- Comentario descriptivo
COMMENT ON DATABASE amazon_books_db IS
    'Base de datos del proyecto final — Dataset Olist Brazilian E-Commerce.
     Pipeline completo de datos con IA — Administración de Datos — LEAD University — Grupo 6.';

\echo '✅ Base de datos amazon_books_db creada exitosamente.'
\echo '   Próximo paso: ejecutar sql/05_create_books_tables.sql'
