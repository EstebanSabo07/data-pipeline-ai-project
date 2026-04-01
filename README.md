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

## Descripción del Proyecto

Pipeline completo de ingeniería de datos que abarca desde la conexión a una base de datos PostgreSQL real con el dataset **Olist Brazilian E-Commerce**, hasta la entrega de una aplicación funcional con un modelo de Inteligencia Artificial para predecir el puntaje de reseña de órdenes de clientes.

### Dataset: Olist Brazilian E-Commerce
- **Fuente:** [Kaggle - Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Descripción:** 1,550,066 registros reales de e-commerce en Brasil (2016–2018)
- **Tablas:** 9 tablas relacionales con datos de clientes, órdenes, productos, vendedores, pagos y reseñas

---

## Arquitectura del Pipeline

```
PostgreSQL (Olist DB)
        |
        v
[PARTE 1] Fuente de Datos Real
  - 9 tablas relacionales
  - Scripts de carga automática
  - Módulo de conexión reutilizable
        |
        v
[PARTE 2] ETL Completo
  - Extracción desde PostgreSQL
  - Transformación y limpieza
  - Carga a capa "curada"
        |
        v
[PARTE 3] Orquestación
  - Ejecución automatizada por etapas
  - Logs de ejecución
        |
        v
[PARTE 4] Aplicativo con IA
  - Modelo de clasificación (review score)
  - App funcional con predicción
```

---

## Estructura del Repositorio

```
pipeline-datos-ia-grupo6/
├── parte1_fuente_datos/       # Esteban Saborío
│   ├── config/
│   ├── sql/
│   ├── scripts/
│   └── README.md
├── parte2_etl/                # [Integrante]
├── parte3_orquestacion/       # [Integrante]
├── parte4_app_ia/             # [Integrante]
├── .gitignore
└── README.md                  # Este archivo
```

---

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
- **Dataset:** Olist Brazilian E-Commerce (Kaggle) — descarga manual
