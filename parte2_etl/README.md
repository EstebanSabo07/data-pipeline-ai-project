**Responsable:** Ariana Víquez S.

---

### 📂 En `parte2_etl/README.md`

```markdown
## Descripción

Esta parte implementa el pipeline de **Extracción, Transformación y Carga (ETL)**. Su objetivo es tomar los datos crudos de la Parte 1, limpiarlos y realizar un **Join** masivo para generar un dataset "Curado" listo para modelos de Inteligencia Artificial.

---

## Proceso de Transformación

| Fase            | Detalle                                                                                                              |
|-----------------|----------------------------------------------------------------------------------------------------------------------|
| **Extracción**  | SQL Query a PostgreSQL (limitado a 100k reseñas para eficiencia en desarrollo).                                      |
| **Limpieza**    | Manejo de nulos en `authors`, normalización de `review_score` y limpieza de caracteres especiales.                   |
| **Integración** | **Inner Join** entre Libros y Reseñas usando el campo `title`.                                                       |
| **Carga**       | Exportación a CSV y creación de la tabla final `books.curated_books_reviews`.                                        |

---

## Estructura de Archivos

parte2_etl/
├── scripts/
│ ├── extract.py ← Lógica de lectura desde PostgreSQL
│ ├── transform.py ← Lógica de Join y Limpieza con Pandas
│ ├── load.py ← Escritura en CSV y Capa Silver de SQL
│ └── run_etl.py ← ORQUESTADOR DEL PIPELINE
├── data/
│ ├── curated/ ← Dataset final: amazon_books_curated.csv
│ └── logs/ ← Registro de errores y tiempos
└── requirements.txt ← Bibliotecas necesarias


---

## Ejecución del Pipeline

```bash
cd parte2_etl
python scripts/run_etl.py
Resultado esperado:

✅ Generación de archivo .csv en data/curated/.
✅ Tabla books.curated_books_reviews disponible en PostgreSQL.

Tecnologías Utilizadas
Pandas: Procesamiento de DataFrames y Joins.
SQLAlchemy: Interfaz de conexión a base de datos.
Logging: Sistema de monitoreo de errores en tiempo real.

---

### 📄 Requirements unificado (mismo para ambos)

Crea el archivo `requirements.txt` en ambas carpetas con este contenido:

```text
pandas==2.2.2
sqlalchemy==2.0.30
psycopg2-binary==2.9.9
python-dotenv==1.0.1
