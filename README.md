# Proyecto Final — Pipeline Medallion de Modelos de IA (Hugging Face)

Pipeline de datos **Medallion (Bronze → Silver → Gold → Semántica)** construido en
**Databricks** sobre **Unity Catalog** que ingesta el catálogo público de modelos de
**Hugging Face** y lo transforma en un **Star Schema** listo para analítica y BI.

El objetivo del proyecto es convertir un snapshot crudo de la API pública de Hugging
Face (`https://huggingface.co/api/models`) en un modelo dimensional confiable que
permita responder preguntas de negocio sobre el ecosistema open-source de IA:
¿qué modelos dominan?, ¿qué tareas crecen más rápido?, ¿qué organizaciones publican
más?, ¿qué licencias predominan?, ¿cuánto se concentra el mercado?

---

## Características principales

- **Ingesta incremental con watermark**: solo se descargan modelos nuevos o
  actualizados desde la última corrida exitosa (tabla de control
  `pf.bronze.ingestion_control`).
- **Idempotencia por diseño**: re-ejecutar el pipeline del mismo día no duplica
  datos (dedup por business key + `replaceWhere` en Bronze/Silver + `MERGE` por
  `row_hash` en Gold).
- **Modelo dimensional completo**: `dim_fecha` (Type 0), dimensiones SCD1 por BK,
  `dim_modelo_scd2` (SCD2 con historial de atributos) y `fact_metricas_diarias`
  con granularidad modelo × día.
- **Capa semántica**: el negocio solo lee vistas `pf.semantic.vw_*` (10 KPIs +
  vista maestra + series temporales), nunca tablas Gold físicas.
- **Gobernanza con Unity Catalog**: RBAC por roles (`data-engineers`,
  `data-analysts`, `business-analysts`).
- **Optimización de rendimiento**: Liquid Clustering, `OPTIMIZE`/ZORDER,
  `ANALYZE TABLE COMPUTE STATISTICS` y `VACUUM` controlado en ventanas de
  `full_refresh`.
- **Data Quality por diseño**: dedup explícito, nulos gestionados, invariantes
  verificables (p. ej. exactamente una versión `is_current = TRUE` por modelo en
  el SCD2) y checks de FKs huérfanas en el hecho.

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Plataforma | Databricks (notebooks + jobs) |
| Catálogo | Unity Catalog (`pf`) |
| Almacenamiento | Delta Lake (tablas) + Volumes (landing CSV) |
| Procesamiento | PySpark / Spark SQL / Python (`requests`) |
| Orquestación | Databricks Jobs (`job-databricks.yml`) |
| BI | Databricks SQL Dashboard (consume `pf.semantic.vw_*`) |

---

## Estructura del repositorio

```
data_engineering_proyecto_final/
├── job-databricks.yml                  # Definición del job (bundle)
├── dahsboard/
│   └── querys-dashboard.sql            # Queries del dashboard (solo vistas semánticas)
├── src/
│   ├── DDL/                            # DDL canónicos (celdas %sql)
│   │   ├── setup.ipynb                 # Catálogo, schemas, volumen, ingestion_control
│   │   ├── bronze/bronze_models_raw.ipynb
│   │   ├── silver/silver_modelos.ipynb
│   │   ├── silver/silver_model_tag.ipynb
│   │   ├── gold/                       # 01_dim_fecha … 09_optimizacion
│   │   └── semantic/                   # 01_vw_modelos_bi, 02_vw_series_temporales,
│   │                                   # 03_vw_kpi, 04_rbac_grants
│   ├── ETL/                            # Lógica de transformación (celdas %py)
│   │   ├── bronze/ingesta_api_landing.ipynb
│   │   ├── bronze/bronze_csv_a_delta.ipynb
│   │   ├── silver/silver_etl.ipynb
│   │   └── gold/                       # 01_gold_dims_scd1 … 04_optimizacion_y_estadisticas
│   └── EDA/                            # Análisis exploratorio por capa
│       ├── bronze/eda_bronze.ipynb
│       ├── silver/eda_silver.ipynb
│       └── gold/eda_gold.ipynb
└── querys_prueba_proyecto_final_ia.dbquery.ipynb
```

> **Nota**: los notebooks se ensamblan manualmente en el workspace de Databricks
> pegando las celdas de cada `.ipynb`; el job los referencia por ruta de workspace.

---

## Arquitectura en una línea

```
Hugging Face API
      │  (requests, paginado por cursor)
      ▼
Landing (Volumen pf.landing.files — CSV por corrida)
      │  (spark.read.csv + schema explícito)
      ▼
Bronze (pf.bronze.models_raw — Delta, particionado por ingestion_date)
      │  (normalización, tipado, dedup)
      ▼
Silver (pf.silver.modelos + pf.silver.model_tag)
      │  (MERGE SCD1/SCD2 + mapeo de SKs)
      ▼
Gold   (Star Schema: dim_fecha, dims SCD1, dim_modelo_scd2, fact_metricas_diarias)
      │  (vistas calculadas)
      ▼
Semántica (pf.semantic.vw_*)  →  Dashboard SQL
```

Detalle completo en [`docs/02_arquitectura.md`](docs/02_arquitectura.md).

---

## Modos de ingesta

El notebook `ingesta_api_landing` acepta el parámetro `mode` (widget):

| Modo | Semántica de paginación | Uso |
|---|---|---|
| `historical` | `sort=downloads` — backfill de las top-N páginas (respeta `max_pages`) | Carga inicial |
| `incremental` | `sort=lastModified` — descarga hasta que toda una página sea anterior al watermark de `pf.bronze.ingestion_control` | Corrida diaria del job |
| `full_refresh` | Igual que `historical` | Snapshot mensual / recarga completa |

El watermark se guarda en `pf.bronze.ingestion_control` (timestamps naive UTC;
el código re-adjunta `tzinfo=UTC`). Parámetros por defecto: `catalog="pf"`,
`mode="incremental"`, `max_pages=50`.

---

## Orquestación (job diario)

Definido en `job-databricks.yml`:

- **Schedule**: todos los días a las 10:00 (zona `America/Argentina/Tucuman`).
- **Cadena de tareas**:
  `ingesta` → `bronze_csv_a_delta` → `eda_bronze` → `etl_bronze_to_silver` →
  `eda_silver` → (`gold_dim_modelo_scd2` + `gold_dims_scd1`) → `gold_fact` →
  (`eda_gold` + `optimizacion`) → `dashboard_refresh`.
- **Notificaciones** por email en start/success/failure.
- El job termina refrescando el dashboard de Databricks SQL.

---

## KPIs disponibles (capa semántica)

| # | Vista | KPI |
|---|---|---|
| 1 | `vw_kpi_modelos_nuevos` | Modelos nuevos por día (tasa de innovación) |
| 2 | `vw_kpi_downloads_acumulados` | Descargas acumuladas e incremento diario |
| 3 | `vw_kpi_ranking_task` | Ranking por tarea (modelos, descargas, descargas/modelo) |
| 4 | `vw_kpi_ranking_org` | Top organizaciones por descargas |
| 5 | `vw_kpi_licencias` | Distribución por licencia |
| 6 | `vw_kpi_licencias_permisivas` | % open-weight (Apache/MIT) sobre modelos nuevos |
| 7 | `vw_kpi_adopcion_semanal` | Adopción semanal (descargas promedio por modelo) |
| 8 | `vw_kpi_ratio_likes` | Ratio likes/descargas (modelos con ≥10k descargas) |
| 9 | `vw_kpi_concentracion` | Concentración del mercado (top-10 %) |
| 10 | `vw_kpi_calidad` | Calidad del pipeline (data quality) |

Explicación detallada en [`docs/03_explicacion_kpis.md`](docs/03_explicacion_kpis.md).

---

## Problemas de negocio que resuelve

El proyecto responde preguntas como: ¿qué modelos y organizaciones dominan el
ecosistema?, ¿qué tareas crecen más rápido?, ¿qué licencias predominan y cuánto
del catálogo es reutilizable (open-weight)?, ¿cuán concentrado está el mercado?,
¿qué modelos tienen mejor relación likes/descargas?

Análisis completo en [`docs/04_problemas_de_negocio.md`](docs/04_problemas_de_negocio.md).

---

## Puesta en marcha (resumen)

1. **Setup**: ejecutar `src/DDL/setup.ipynb` (crea catálogo `pf`, schemas
   `bronze/silver/gold/semantic/landing`, volumen `pf.landing.files` y la tabla
   `pf.bronze.ingestion_control`).
2. **DDL**: ejecutar los DDL de Bronze, Silver, Gold y Semántica en orden.
3. **Carga inicial**: correr `ingesta_api_landing` con `mode=historical`
   (o `full_refresh`).
4. **Pipeline**: correr el job `proyecto_final_ia_models_job` (o cada ETL en
   orden: bronze → silver → gold → semántica).
5. **Dashboard**: las queries de `dahsboard/querys-dashboard.sql` consumen
   exclusivamente `pf.semantic.vw_*`.

> **Importante**: el bundle `job-databricks.yml` contiene un host de workspace
> placeholder y requiere la variable `sql_warehouse_id`; no se deben agregar
> credenciales reales al repositorio.

---

## Documentación relacionada

- [`docs/02_arquitectura.md`](docs/02_arquitectura.md) — arquitectura y flujo de datos
- [`docs/03_explicacion_kpis.md`](docs/03_explicacion_kpis.md) — definición de KPIs
- [`docs/04_problemas_de_negocio.md`](docs/04_problemas_de_negocio.md) — casos de uso de negocio