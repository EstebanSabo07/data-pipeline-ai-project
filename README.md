# Pipeline Completo de Datos con App de IA
## Proyecto Final Grupal — Administración de Datos
### Bachillerato en Ingeniería en Ciencia de Datos — LEAD University

---

**Grupo 6**
- Ariana Víquez Solano
- Esteban Gutiérrez Saborío
- Jasser Andiell Palacios Olivas
- Marco Chinchilla Barrantes

**Profesor:** Alejandro Zamora

---

# Parte 1: Fuente de Datos - Amazon Books Reviews

Este módulo se encarga de la infraestructura inicial y la ingesta de datos crudos (Raw Data) desde archivos CSV hacia una base de datos relacional PostgreSQL.

## Tecnologías
- **Base de Datos:** PostgreSQL
- **Lenguaje:** Python 3.12
- **Librerías:** Pandas (Ingesta), SQLAlchemy (ORM), Psycopg2 (Driver).

## Estructura de Datos
Se configuró un esquema llamado `books` con dos tablas principales:
1. `books_data`: Catálogo de 212,404 libros (títulos, autores, categorías).
2. `books_rating`: ~3 millones de reseñas (usuarios, puntajes, textos de reseñas).

## Instrucciones de Ejecución
1. Asegúrese de tener el archivo `.env` configurado en `config/` con las credenciales de la DB.
2. Coloque los archivos `books_data.csv` y `Books_rating.csv` en `data/raw/`.
3. Ejecute el orquestador inicial:
   ```bash
   python scripts/run_parte1.py

4. Verifique la carga con el script de validación:
   ```bash
         python scripts/verify_load.py
   # Parte 2: Proceso ETL - Curación de Datos

Este módulo implementa el proceso de Extracción, Transformación y Carga (ETL) para preparar los datos de Amazon Books para un modelo de IA.

## Lógica del Pipeline
1. **Extracción:** Consulta las tablas `books_data` y `books_rating` desde PostgreSQL.
2. **Transformación:**
   - Limpieza de valores nulos en títulos y autores.
   - Normalización de formatos de fecha.
   - **Join:** Unión de catálogos y reseñas mediante la columna `title`.
   - **Filtrado:** Selección de columnas relevantes para entrenamiento de IA.
3. **Carga:** Persistencia de los datos "curados" en una nueva tabla `books.curated_books_reviews` y un archivo CSV para análisis rápido.

## Ejecución
Para iniciar el proceso de transformación:
```bash
python scripts/run_etl.py

## Cómo Ejecutar el Proyecto

### 1. Clonar el repositorio
```bash
git clone https://github.com/EstebanSabo07/data-pipeline-ai-admin-datos-final-project.git
cd data-pipeline-ai-admin-datos-final-project
```

### 2. Configurar la Parte 1 (Fuente de Datos)
Ver instrucciones detalladas en [`parte1_fuente_datos/README.md`](parte1_fuente_datos/README.md)

### 3. Ejecutar ETL (Parte 2)
Ver instrucciones en `parte2_etl/README.md`

### 4. Orquestación (Parte 3)
Ver instrucciones en `parte3_orquestacion/README.md`

### 5. Aplicativo IA (Parte 4)
Ver instrucciones en `parte4_app_ia/README.md`

---

## Tecnologías Utilizadas

- **Base de Datos:** PostgreSQL 18 (Postgres.app)
- **Lenguaje:** Python 3.13 (Miniconda)
- **Librerías:** psycopg2-binary 2.9.10, SQLAlchemy 2.0.30, pandas 2.2.2, scikit-learn
- **Dataset:** Amazon Books Reviews
Goodreads-books reviews and descriptions of each book (Kaggle) — descarga manual
