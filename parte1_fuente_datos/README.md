# Parte 1: Fuente de Datos Real — PostgreSQL
## Pipeline Completo de Datos con IA | Grupo 6 | LEAD University

**Responsable:** Esteban Gutiérrez Saborío
**Curso:** Administración de Datos
**Profesor:** Alejandro Zamora

---

## Descripción

Esta parte implementa la **fuente de datos real** del pipeline utilizando PostgreSQL como motor de base de datos relacional. Se utiliza el dataset **Olist Brazilian E-Commerce** con más de 1,550,000 registros distribuidos en **9 tablas relacionales** con claves primarias, foráneas, índices y restricciones de integridad.

---

## Dataset: Olist Brazilian E-Commerce

| Campo           | Detalle                                                                                                              |
|-----------------|----------------------------------------------------------------------------------------------------------------------|
| Fuente          | [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| Período         | Septiembre 2016 — Agosto 2018                                                                                        |
| Registros       | 1,550,066 registros totales / ~99,000 clientes únicos                                                               |
| Tablas          | 9 tablas relacionales                                                                                                |
| Variable Target | `review_score` (1–5 estrellas) → modelo IA Parte 4                                                                  |

### Esquema de la Base de Datos

```
olist_db
└── schema: olist
    ├── product_category_name_translation  (71 categorías)
    ├── customers                          (99,441 clientes)
    ├── geolocation                        (984,024 coordenadas)
    ├── sellers                            (3,095 vendedores)
    ├── products                           (32,951 productos)
    ├── orders             ◄── TABLA CENTRAL (99,441 órdenes)
    │   ├── order_items    (112,650 ítems)
    │   ├── order_payments (103,886 pagos)
    │   └── order_reviews  (99,224 reseñas) ← TARGET del modelo IA
```

### Diagrama de Relaciones

```
customers ──────────── orders ──────────── order_items ──── products ── product_category_name_translation
                          │                     │
                          │                     └──────────── sellers
                          ├── order_payments
                          └── order_reviews
```

---

## Estructura de Archivos

```
parte1_fuente_datos/
├── config/
│   ├── .env.example        ← Plantilla de credenciales (copiar como .env)
│   └── db_config.py        ← Módulo de configuración (lee del .env)
├── sql/
│   ├── 01_create_database.sql  ← Script SQL para crear olist_db
│   ├── 02_create_tables.sql    ← 9 tablas con PK, FK y constraints
│   ├── 03_create_indexes.sql   ← Índices para optimización de queries
│   └── 04_verify_schema.sql    ← Queries de verificación del esquema
├── scripts/
│   ├── db_connection.py    ← Módulo de conexión (reutilizable por ETL)
│   ├── download_dataset.py ← Verificación de archivos del dataset
│   ├── load_data.py        ← Carga CSV → PostgreSQL (con limpieza)
│   ├── verify_load.py      ← Verificación post-carga + reporte de auditoría
│   └── run_parte1.py       ← SCRIPT MAESTRO (ejecuta todo el pipeline)
├── data/
│   ├── raw/               ← CSVs de Kaggle (git-ignorados, ~120 MB)
│   ├── reports/           ← Reportes de verificación auto-generados
│   └── logs/              ← Logs de ejecución del pipeline
└── requirements.txt       ← Dependencias Python
```

---

## Instrucciones de Configuración

### Prerequisitos

- PostgreSQL 15+ instalado y en ejecución (probado con PostgreSQL 18 / Postgres.app)
- Python 3.10+ (probado con Python 3.13 / Miniconda)

### Paso 1: Instalar dependencias

```bash
cd parte1_fuente_datos
pip install -r requirements.txt
```

### Paso 2: Configurar credenciales de la base de datos

```bash
# Copiar la plantilla de variables de entorno
cp config/.env.example .env

# Editar .env con tus credenciales reales
nano .env   # o cualquier editor de texto
```

Contenido mínimo del `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=olist_db
DB_USER=tu_usuario_postgres
DB_PASSWORD=tu_password
```

> Si usás **Postgres.app** en macOS, el usuario es tu usuario del sistema y la contraseña puede quedar vacía.

### Paso 3: Crear la base de datos en PostgreSQL

```bash
# Opción A — con psql
createdb olist_db

# Opción B — con el script SQL
psql -U postgres -f sql/01_create_database.sql
```

### Paso 4: Descargar el dataset Olist

El dataset se descarga manualmente desde Kaggle (requiere cuenta gratuita):

1. Ve a: **https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce**
2. Click en el botón **"Download"** (esquina superior derecha)
3. Extrae el `.zip` descargado
4. Copia los **9 archivos CSV** a la carpeta `data/raw/`

Los archivos esperados son:
```
data/raw/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

### Paso 5: Ejecutar el pipeline completo

```bash
cd parte1_fuente_datos
python scripts/run_parte1.py
```

El script maestro ejecuta automáticamente:
1. ✅ Test de conexión a PostgreSQL
2. ✅ Verificación del dataset (9/9 archivos presentes)
3. ✅ Creación del esquema `olist` (9 tablas + índices)
4. ✅ Carga de todos los CSV en PostgreSQL (~1.5M registros)
5. ✅ Verificación post-carga con reporte de auditoría

**Tiempo estimado:** ~50 segundos en hardware moderno.

---

## Ejecución Modular (alternativa)

Si preferís ejecutar cada componente por separado:

```bash
# 1. Test de conexión
python scripts/db_connection.py

# 2. Verificar archivos del dataset
python scripts/download_dataset.py

# 3. Cargar datos a PostgreSQL
python scripts/load_data.py

# 4. Generar reporte de verificación
python scripts/verify_load.py
```

---

## Integración con la Parte 2 (ETL)

El módulo `db_connection.py` está diseñado para ser **reutilizable** por el ETL (Parte 2) y la aplicación IA (Parte 4).

```python
# En los scripts de la Parte 2:
import sys
sys.path.insert(0, '../parte1_fuente_datos')
from scripts.db_connection import get_database_connection

db = get_database_connection()
engine = db.get_engine()

# Extraer datos con pandas
import pandas as pd
df_reviews = pd.read_sql("SELECT * FROM olist.order_reviews LIMIT 1000", engine)
```

**String de conexión SQLAlchemy:**
```
postgresql+psycopg2://USUARIO:PASSWORD@localhost:5432/olist_db
```

---

## Evidencia de Carga

Después de ejecutar el pipeline, la verificación confirma:

| Tabla                             | Registros   |
|-----------------------------------|-------------|
| olist.geolocation                 | 984,024     |
| olist.order_reviews               | 99,224      |
| olist.orders                      | 99,441      |
| olist.order_payments              | 103,886     |
| olist.order_items                 | 112,650     |
| olist.customers                   | 99,441      |
| olist.products                    | 32,951      |
| olist.sellers                     | 3,095       |
| olist.product_category_name_translation | 71     |
| **TOTAL**                         | **1,550,066** |

- ✅ 0 valores nulos en columnas clave
- ✅ 0 registros huérfanos (integridad referencial)
- ✅ Rango temporal: sept 2016 — oct 2018 (25 meses)

---

## Tecnologías Utilizadas

| Tecnología   | Versión | Uso                                |
|--------------|---------|-------------------------------------|
| PostgreSQL   | 18      | Motor de base de datos relacional   |
| Python       | 3.13    | Scripts de carga y verificación     |
| psycopg2     | 2.9.10  | Driver PostgreSQL nativo            |
| SQLAlchemy   | 2.0.30  | Engine para integración con pandas  |
| pandas       | 2.2.2   | Lectura de CSV y transformaciones   |
| python-dotenv| 1.0.1   | Gestión segura de credenciales      |





## Nota sobre el Dataset

El dataset no está incluido en este repositorio debido a su tamaño (~120 MB).

Debe descargarse manualmente desde Kaggle y colocarse en:
parte1_fuente_datos/data/raw/
