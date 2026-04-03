# Parte 1: Fuente de Datos Real — PostgreSQL
## Pipeline Completo de Datos con IA | Grupo 6 | LEAD University

**Responsable:** Esteban Gutiérrez Saborío
**Curso:** Administración de Datos
**Profesor:** Alejandro Zamora

---

## Descripción

Esta parte implementa la **fuente de datos real** del pipeline utilizando PostgreSQL como motor de base de datos relacional. Se utiliza el dataset **Amazon Books Reviews** con más de 3,000,000 de registros distribuidos en **2 tablas relacionales** con esquemas definidos, tipos de datos optimizados e índices.

---

## Dataset: Amazon Books Reviews

| Campo           | Detalle                                                                                                              |
|-----------------|----------------------------------------------------------------------------------------------------------------------|
| Fuente          | [Kaggle — Amazon Books Reviews](https://www.kaggle.com/datasets/mohamedbakheet/amazon-books-reviews) |
| Registros       | ~3,212,404 registros totales                                                                                        |
| Tablas          | 2 tablas relacionales (`books_data`, `books_rating`)                                                                |
| Variable Target | `review_score` (1–5 estrellas) → modelo IA Parte 4                                                                  |

### Esquema de la Base de Datos
amazon_books_db
└── schema: books
├── books_data (212,404 libros: autores, categorías, descripción)
└── books_rating (3,000,000+ reseñas: usuarios, scores, textos) ← TARGET


---

## Estructura de Archivos
parte1_fuente_datos/
├── config/
│ ├── .env.example ← Plantilla de credenciales
│ └── db_config.py ← Módulo de configuración (lee del .env)
├── sql/
│ ├── 01_create_database.sql ← Script para crear amazon_books_db
│ └── 05_create_books_tables.sql ← Tablas con tipos TEXT y NUMERIC
├── scripts/
│ ├── db_connection.py ← Módulo de conexión reutilizable
│ ├── load_books_data.py ← Carga CSV → PostgreSQL (con mapeo de columnas)
│ ├── verify_load.py ← Reporte de auditoría post-carga
│ └── run_parte1.py ← SCRIPT MAESTRO
├── data/
│ ├── raw/ ← CSVs de Kaggle (git-ignorados)
│ └── logs/ ← Logs de ejecución
└── requirements.txt ← Dependencias Python


---

## Instrucciones de Configuración

# Paso 1: Instalar dependencias
```bash
cd parte1_fuente_datos
pip install -r requirements.txt


# Paso 2: Configurar credenciales (.env)
Contenido mínimo:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=amazon_books_db
DB_USER=postgres
DB_PASSWORD=tu_password


# Paso 3: Descargar el dataset
Descarga desde: https://www.kaggle.com/datasets/mohamedbakheet/amazon-books-reviews
Coloca los archivos en data/raw/:
books_data.csv
Books_rating.csv


# Paso 4: Ejecutar el pipeline
python scripts/run_parte1.py

Evidencia de Carga
Tabla	Registros	Estado
books.books_data	212,404	✅ OK
books.books_rating	3,000,000+	✅ OK




## Nota sobre el Dataset

El dataset no está incluido en este repositorio debido a su tamaño (~120 MB).

Debe descargarse manualmente desde Kaggle y colocarse en:
parte1_fuente_datos/data/raw/
