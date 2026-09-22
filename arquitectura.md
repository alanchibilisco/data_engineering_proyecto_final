# Arquitectura del Proyecto

Pipeline **Medallion** en Databricks sobre Unity Catalog (catálogo `pf`) que
ingesta el catálogo público de modelos de Hugging Face y lo transforma en un
**Star Schema** Gold + capa semántica para BI.

---

## 1. Vista general del flujo de datos

```
┌─────────────────────┐
│  Hugging Face API   │  GET https://huggingface.co/api/models
│  (catálogo público) │  params: limit=1000, full=true, sort, direction, cursor
└──────────┬──────────┘
           │  requests + paginado por cursor (header Link: rel="next")
           │  reintentos/backoff ante 429/5xx
           ▼
┌─────────────────────┐
│  LANDING            │  Volumen pf.landing.files
│  CSV por corrida    │  /run_id=<run_id>/page_00001.csv … page_NNNNN.csv
│  (crudo, sin tocar) │  + registro en pf.bronze.ingestion_control (watermark)
└──────────┬──────────┘
           │  spark.read.csv (schema explícito, PERMISSIVE, multiLine)
           ▼
┌─────────────────────┐
│  BRONZE             │  pf.bronze.models_raw
│  Delta, crudo       │  particionado por ingestion_date
│  append/replaceWhere│  dedup por (_id, ingestion_date)
└──────────┬──────────┘
           │  normalización + tipado + dedup por (model_id, ingestion_date)
           ▼
┌─────────────────────┐
│  SILVER             │  pf.silver.modelos
│  limpio y tipado    │  pf.silver.model_tag (tags explotados)
│  replaceWhere       │  particionado por ingestion_date
└──────────┬──────────┘
           │  MERGE SCD1/SCD2 + mapeo de SKs + cálculo de métricas
           ▼
┌─────────────────────┐
│  GOLD (Star Schema) │  dim_fecha (T0) · dims SCD1 · dim_modelo_scd2 (SCD2)
│  modelado           │  fact_metricas_diarias (modelo × día)
└──────────┬──────────┘
           │  vistas calculadas (solo lectura para negocio)
           ▼
┌─────────────────────┐
│  SEMÁNTICA          │  pf.semantic.vw_modelos_bi
│  vistas de negocio  │  pf.semantic.vw_series_temporales
│                     │  pf.semantic.vw_kpi_* (10 KPIs)
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│  DASHBOARD SQL      │  Databricks SQL Dashboard
│  (solo vw_*)        │  dahsboard/querys-dashboard.sql
└─────────────────────┘
```

---

## 2. Capa Landing (volumen)

**Notebook**: `src/ETL/bronze/ingesta_api_landing.ipynb`

- Descarga la API de HF con paginado por cursor (`limit=1000`, `full=true`).
- Escribe **una página por CSV** en el Volumen `pf.landing.files/run_id=<run_id>/`.
- Cada fila conserva el **payload JSON íntegro** (`payload_json`) como backup
  y trazabilidad, más metadatos de corrida (`ingesta_run_id`, `ingesta_mode`,
  `page_no`, `ingestion_ts`, `ingestion_date`).
- Registra cada corrida en `pf.bronze.ingestion_control`:
  `run_id`, `mode`, `start_ts`, `end_ts`, `watermark_before`, `watermark_after`,
  `n_pages`, `n_records`, `status`, `notes`.

### Modos de ingesta

| Modo | sort | Lógica de parada |
|---|---|---|
| `historical` | `downloads` | Descarga hasta `max_pages` (default 50) |
| `incremental` | `lastModified` | Se detiene cuando **toda** una página es anterior al watermark (cota de seguridad: 200 páginas) |
| `full_refresh` | `downloads` | Igual que `historical` (snapshot mensual) |

El watermark (`watermark_after`) se calcula como el máximo `lastModified`
observado en la corrida (o el timestamp de corrida). Spark devuelve timestamps
**native UTC**; el código re-adjunta `tzinfo=UTC` antes de comparar.

---

## 3. Capa Bronze

**DDL**: `src/DDL/bronze/bronze_models_raw.ipynb`
**ETL**: `src/ETL/bronze/bronze_csv_a_delta.ipynb`

- **Tabla**: `pf.bronze.models_raw`, Delta, particionada por `ingestion_date`.
- **Lectura**: `spark.read.csv` con **schema explícito** (todas las columnas
  string en origen), `mode=PERMISSIVE`, `multiLine=true` (los JSON embebidos
  pueden contener saltos de línea).
- **Dedup por business key**: `(_id, ingestion_date)` — un modelo puede aparecer
  en varias páginas de la misma corrida (el ranking cambia entre peticiones) y
  en varias corridas. Se conserva la fila más reciente por `lastModified`,
  desempate por `ingestion_ts` y `page_no` (determinista).
- **Escritura idempotente**: `overwrite` + `replaceWhere` sobre las particiones
  `ingestion_date` presentes en la corrida → re-ejecutar el mismo día no duplica.
- **TBLPROPERTIES**: `delta.enableChangeDataFeed=true`,
  `autoOptimize.optimizeWrite/autoCompact=true`.

> ⚠️ **Regla de oro**: cualquier cambio en Bronze obliga a re-correr Silver de
> esos días (relación `partitionBy ingestion_date` ↔ `replaceWhere`).

---

## 4. Capa Silver

**DDL**: `src/DDL/silver/silver_modelos.ipynb`, `silver_model_tag.ipynb`
**ETL**: `src/ETL/silver/silver_etl.ipynb`

### `pf.silver.modelos`
Normalización con `spark.sql`:
- `model_id` = `id` (BK natural `org/nombre`), `nombre` = parte tras `/`,
  `org_id` = parte delantera.
- Casting: `likes`/`downloads` → `BIGINT`, `private` → `BOOLEAN`,
  `createdAt`/`lastModified` → `TIMESTAMP`.
- `NULLIF` para `pipeline_tag`/`library_name` vacíos.
- `license_tag` extraído de los tags (`filter(from_json(tags,...), t LIKE 'license:%')`).
- `has_metadata` = `payload_json IS NOT NULL` (marcador de calidad).
- Dedup por `(model_id, ingestion_date)` con DataFrame API.
- Escritura `overwrite` + `replaceWhere` por día.

### `pf.silver.model_tag`
- Tags **explotados** (`explode(from_json(tags, 'array<string>'))`): una fila por
  (modelo, tag).
- Flags `es_license`, `es_dataset`, `es_arxiv` para clasificar tags.
- Misma estrategia de escritura particionada por `ingestion_date`.

---

## 5. Capa Gold (Star Schema)

**DDL**: `src/DDL/gold/*` · **ETL**: `src/ETL/gold/*`

### 5.1 Dimensiones

| Dimensión | Tipo | BK | Notas |
|---|---|---|---|
| `dim_fecha` | **Type 0** | `fecha_id` (AAAAMMDD) | Calendario estático 2020–2032, poblado por MERGE idempotente |
| `dim_organizacion` | SCD1 | `org_id` | `num_modelos_ref` recalculable |
| `dim_task` | SCD1 | `pipeline_tag` | Descripción funcional por CASE (LLM, clasificación, embeddings…) |
| `dim_libreria` | SCD1 | `library_name` | Descripción legible (transformers, sentence-transformers, vLLM…) |
| `dim_licencia` | SCD1 | `license_tag` | `es_permissiva` = Apache-2.0/MIT (open-weight); `sin_licencia` para NULL |
| `dim_tag` | SCD1 | `tag` | `tipo_tag`: license / dataset / arxiv / generico |
| `dim_modelo_scd2` | **SCD2** | `model_id` | `valid_from`/`valid_to`/`is_current`; `valid_to='9999-12-31'` para vigentes |

**SCD2 (`dim_modelo_scd2`)** — dos operaciones por corrida:
1. **Cerrar** la versión vigente si cambia `nombre`, `org_id`, `pipeline_tag`,
   `library_name` o `license_tag` (`valid_to = CURRENT_TIMESTAMP()`,
   `is_current = FALSE`).
2. **Insertar** la nueva versión vigente si no existe una (`valid_to='9999-12-31'`,
   `is_current = TRUE`).

**Invariante verificable**: cada `model_id` debe tener **exactamente una** fila
`is_current = TRUE` (check en ETL y en EDA Gold).

### 5.2 Tabla de hechos

**`pf.gold.fact_metricas_diarias`** — granularidad **modelo × día** (snapshot).

- **PK**: `row_hash = MD5(CONCAT_WS('|', model_id, fecha_id, modelo_id))` →
  MERGE idempotente (re-correr el día no duplica).
- **FKs**: `fecha_id`, `created_fecha_id` → `dim_fecha`; `modelo_id` →
  `dim_modelo_scd2` (versión vigente del día); `org_sk` → `dim_organizacion`;
  `task_id` → `dim_task`; `libreria_id` → `dim_libreria`; `licencia_id` →
  `dim_licencia`. FKs declaradas como constraints en el DDL.
- **Métricas**:
  - `likes`, `downloads`: snapshot del día.
  - `delta_downloads`: `downloads_hoy - downloads_día_anterior` (es backfill
    para `fecha_id = hoy`; `NULL`/0 el primer día).
  - `es_primer_dia`: `TRUE` si el modelo aparece por primera vez (se usa para
    contar modelos nuevos sin duplicar snapshots).
- **Staging**: toma el snapshot más reciente de Silver (`MAX(ingestion_date)`),
  mapea SKs por BK con joins a las dimensiones (LEFT JOIN para task/librería/
  licencia, que pueden ser NULL).

### 5.3 Optimización

**DDL**: `src/DDL/gold/09_optimizacion.ipynb`
**ETL**: `src/ETL/gold/04_optimizacion_y_estadisticas.ipynb`

- **Liquid Clustering** en `fact_metricas_diarias` por `(model_id, fecha_id)`.
- `OPTIMIZE` en el hecho y `ZORDER BY (model_id, is_current)` en
  `dim_modelo_scd2`; `OPTIMIZE` en Silver.
- `ANALYZE TABLE … COMPUTE STATISTICS` en todas las tablas Gold.
- `VACUUM RETAIN 168 HOURS` **solo** cuando `mode=full_refresh` (mantenimiento
  mensual/trimestral), nunca en incremental.

---

## 6. Capa Semántica

**DDL**: `src/DDL/semantic/*`

| Vista | Contenido |
|---|---|
| `vw_modelos_bi` | Vista maestra: modelo, nombre, organización, fecha, fecha_creación, descargas, likes, delta, tarea, librería, licencia (oculta los joins del star schema) |
| `vw_series_temporales` | Serie temporal por modelo: fecha (snapshot), fecha_creación, tarea, descargas, likes, delta |
| `vw_kpi_*` (10 vistas) | Ver [`./_explicacion_kpis.md`](explicacion_kpis.md) |

**Regla**: el dashboard **solo** lee `pf.semantic.vw_*`, nunca tablas Gold
físicas.

---

## 7. Gobernanza (RBAC)

**DDL**: `src/DDL/semantic/04_rbac_grants.ipynb`

| Rol | Acceso |
|---|---|
| `data-engineers` | `USE CATALOG` + `USE SCHEMA` + `CREATE TABLE` + `MODIFY` + `SELECT` en bronze/silver/gold/semantic + `READ VOLUME` en `pf.landing.files` |
| `data-analysts` | `SELECT` en silver y gold |
| `business-analysts` | `SELECT` **solo** en `pf.semantic` |

---

## 8. Orquestación (Databricks Jobs)
```
ingesta (mode=incremental)
  └─ bronze_csv_a_delta
       └─ eda_bronze
            └─ etl_bronze_to_silver
                 └─ eda_silver
                      ├─ gold_dim_modelo_scd2
                      └─ gold_dims_scd1
                           └─ gold_fact
                                ├─ eda_gold
                                └─ optimizacion
                                     └─ dashboard_refresh
```

- **Schedule**: `0 0 10 * * ?` (10:00, `America/Argentina/Tucuman`), activo.
- **Notificaciones**: email en start/success/failure.
- **Dashboard**: la última tarea refresca el dashboard SQL

---

## 9. Decisiones de diseño clave

1. **Snapshots diarios en Bronze/Silver** (particionados por `ingestion_date`)
   en lugar de solo estado actual: permite series temporales y `delta_downloads`
   sin re-consultar la API.
2. **`payload_json` íntegro en Bronze**: re-procesamiento sin re-llamar a la API
   (trazabilidad y resiliencia).
3. **SCD2 solo en `dim_modelo_scd2`**: los atributos del modelo (tarea, librería,
   licencia) cambian con el tiempo y afectan al análisis histórico; las demás
   dimensiones son SCD1 porque su atributo es el propio BK.
4. **`row_hash` como PK del hecho**: MERGE idempotente y estable aunque cambien
   las SKs (p. ej. nueva versión SCD2 del modelo).
5. **`es_primer_dia` + `created_fecha_id`**: los "modelos nuevos" se cuentan por
   fecha de **creación real** (`createdAt`), no por fecha de ingesta.
6. **`delta_downloads` backfill solo para hoy**: evita reescribir historia y
   mantiene el hecho append-only salvo el día en curso.
7. **VACUUM solo en `full_refresh`**: preserva el CDF y el historial Delta para
   auditoría durante las corridas incrementales.

---

## 10. Flujo de verificación (calidad)

Cada ETL termina con checks `spark.sql(...).show()`:

- **Bronze**: conteo por partición, `COUNT(*)` vs `COUNT(DISTINCT _id)` por día
  (dedup correcto).
- **Silver**: total, `has_metadata`, `sin_licencia`, `modelos_unicos`; EDA con
  nulos, percentiles de descargas, buckets de long-tail.
- **Gold**: FKs sin huérfanas, `row_hash` únicos, invariante SCD2
  (una vigente por modelo), previews de KPIs.
- **Semántica**: preview de cada `vw_kpi_*`.

Ver también `src/EDA/*` (bronze, silver, gold).