# Explicación de los KPIs

La capa semántica (`pf.semantic`) expone **10 KPIs** como vistas SQL listas para
consumir desde el dashboard o cualquier herramienta de BI. Todos los KPIs se
calculan sobre el Star Schema Gold (`fact_metricas_diarias` + dimensiones), lo
que garantiza consistencia de definición.

> **Convenciones del modelo**
> - `fecha_id` = fecha de **ingesta/snapshot** (cuándo se observó el dato).
> - `created_fecha_id` = fecha de **creación real** del modelo (`createdAt` de la API).
> - `es_primer_dia = TRUE` marca la primera aparición de un modelo (evita
>   duplicar al contar "modelos nuevos" en snapshots posteriores).
> - `delta_downloads` = descargas de hoy − descargas del día anterior.

---

## KPI 1 — Modelos nuevos por día (tasa de innovación)

**Vista**: `vw_kpi_modelos_nuevos`

```sql
SELECT 
       created_fecha_id AS fecha_id,
       COUNT(*) AS modelos_nuevos
FROM pf.gold.fact_metricas_diarias
WHERE es_primer_dia = TRUE
GROUP BY created_fecha_id;
```

**Definición**: número de modelos que se **crearon** cada día (según `createdAt`),
contando cada modelo una sola vez (gracias a `es_primer_dia`).

**Interpretación**: mide el **ritmo de innovación** del ecosistema open-source.
Una curva creciente indica aceleración en la publicación de modelos; picos
puntuales suelen coincidir con lanzamientos de frameworks o eventos de la
comunidad.

**Uso en dashboard**: gráfico de barras/línea "modelos nuevos por día".

---

## KPI 2 — Descargas acumuladas e incremento diario

**Vista**: `vw_kpi_downloads_acumulados`

```sql
SELECT 
       df.fecha,
       SUM(f.downloads) AS descargas_snapshot,
       SUM(f.delta_downloads) AS incremento_del_dia
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_fecha df ON df.fecha_id = f.fecha_id
GROUP BY df.fecha;
```

**Definición**:
- `descargas_snapshot`: suma de descargas acumuladas de todos los modelos
  observados ese día (stock).
- `incremento_del_dia`: suma de `delta_downloads` (flujo nuevo de descargas
  respecto al día anterior).

**Interpretación**: el snapshot crece de forma monótona (las descargas son
acumulativas); el incremento diario muestra el **flujo real de adopción**.
Un incremento estable o creciente indica demanda sostenida; una caída abrupta
puede señalar saturación o problemas de medición.

**Uso en dashboard**: métrica de cabecera (`descargas_totales`) y serie temporal.

---

## KPI 3 — Ranking por tarea (pipeline_tag)

**Vista**: `vw_kpi_ranking_task`

```sql
SELECT 
       dt.pipeline_tag AS tarea,
       COUNT(DISTINCT f.model_id) AS modelos,
       SUM(f.downloads) AS descargas,
       ROUND(SUM(f.downloads) / NULLIF(COUNT(DISTINCT f.model_id), 0), 2) AS descargas_por_modelo
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_task dt ON dt.task_id = f.task_id
GROUP BY dt.pipeline_tag
ORDER BY descargas DESC;
```

**Definición**: por tarea (text-generation, text-classification, image-classification,
automatic-speech-recognition, etc.): nº de modelos, descargas totales y
descargas promedio por modelo.

**Interpretación**: identifica **qué tipo de IA domina la demanda** y qué tareas
están saturadas (muchos modelos, pocas descargas por modelo) frente a nichos con
alta demanda relativa. `descargas_por_modelo` es el indicador de eficiencia:
una tarea con pocos modelos pero alto promedio indica oportunidad.

**Uso en dashboard**: ranking/tabla "ranking por tarea".

---

## KPI 4 — Top organizaciones por descargas

**Vista**: `vw_kpi_ranking_org`

```sql
SELECT 
       do.org_id AS organizacion,
       SUM(f.downloads) AS descargas,
       COUNT(DISTINCT f.model_id) AS modelos_publicados
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_organizacion do ON do.org_sk = f.org_sk
GROUP BY do.org_id
ORDER BY descargas DESC
LIMIT 20;
```

**Definición**: organizaciones ordenadas por descargas totales, con el nº de
modelos publicados.

**Interpretación**: muestra **quiénes lideran el ecosistema** (Meta, Google,
Mistral, OpenAI-equivalents open, etc.). La comparación descargas vs. modelos
distingue entre organizaciones con pocos modelos muy usados (calidad/impacto) y
organizaciones con muchos modelos de menor adopción (volumen).

**Uso en dashboard**: ranking "top organizaciones por descargas".

---

## KPI 5 — Distribución por licencia

**Vista**: `vw_kpi_licencias`

```sql
SELECT 
       dl.license_tag AS licencia,
       COUNT(DISTINCT f.model_id) AS modelos,
       SUM(f.downloads) AS descargas,
       ROUND(SUM(f.downloads) / (SELECT SUM(downloads) FROM pf.gold.fact_metricas_diarias) * 100, 2) AS pct_descargas
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_licencia dl ON dl.licencia_id = f.licencia_id
GROUP BY dl.license_tag
ORDER BY descargas DESC;
```

**Definición**: distribución de modelos y descargas por licencia
(`license:apache-2.0`, `license:mit`, `license:llama2`, `sin_licencia`, etc.),
con el % de descargas que representa cada licencia.

**Interpretación**: clave para **evaluar riesgo legal y reutilización**. Una alta
concentración de descargas en licencias permisivas (Apache/MIT) indica un
ecosistema mayormente reutilizable; una concentración en licencias restrictivas
o sin licencia declarada implica cautela para uso comercial.

**Uso en dashboard**: gráfico de torta/barras "distribución por licencia".

---

## KPI 6 — Licencias permisivas (% open-weight) sobre modelos nuevos

**Vista**: `vw_kpi_licencias_permisivas`

```sql
SELECT 
       COUNT(*) AS total_modelos_nuevos,
       SUM(CASE WHEN dl.es_permissiva THEN 1 ELSE 0 END) AS modelos_permisivos,
       ROUND(100 * SUM(CASE WHEN dl.es_permissiva THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_permisivos
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_licencia dl ON dl.licencia_id = f.licencia_id
WHERE f.es_primer_dia = TRUE;
```

**Definición**: sobre los modelos **nuevos** (primera aparición), qué porcentaje
tiene licencia **open-weight permisiva** (Apache-2.0 o MIT).

**Interpretación**: tendencia de la comunidad hacia la apertura. Un `pct_permisivos`
alto y creciente indica que el ecosistema se vuelve más reutilizable; si baja,
los modelos nuevos tienden a licencias restrictivas (riesgo para adopción
comercial).

**Uso en dashboard**: métrica de cabecera (`total_modelos`) y % de open-weight.

---

## KPI 7 — Adopción semanal (descargas promedio por modelo)

**Vista**: `vw_kpi_adopcion_semanal`

```sql
SELECT 
       DATE_TRUNC('week', df.fecha) AS semana,
       ROUND(AVG(f.downloads), 2) AS descargas_promedio_por_modelo,
       COUNT(DISTINCT f.model_id) AS modelos_activos
FROM pf.gold.fact_metricas_diarias f
JOIN pf.gold.dim_fecha df ON df.fecha_id = f.fecha_id
GROUP BY DATE_TRUNC('week', df.fecha);
```

**Definición**: por semana, promedio de descargas por modelo y nº de modelos
activos (observados).

**Interpretación**: suaviza el ruido diario. El promedio por modelo sube cuando
los modelos existentes ganan adopción; `modelos_activos` crece con la entrada de
nuevos modelos. La combinación permite distinguir crecimiento por **profundidad**
(más descargas por modelo) vs. **amplitud** (más modelos).

**Uso en dashboard**: serie temporal semanal de adopción.

---

## KPI 8 — Ratio likes / descargas (modelos relevantes)

**Vista**: `vw_kpi_ratio_likes`

```sql
SELECT 
       f.model_id,
       MAX(f.likes) AS likes,
       MAX(f.downloads) AS downloads,
       ROUND(MAX(f.likes) / NULLIF(MAX(f.downloads), 0), 5) AS ratio_likes_descargas
FROM pf.gold.fact_metricas_diarias f
GROUP BY f.model_id
HAVING MAX(f.downloads) >= 10000
ORDER BY ratio_likes_descargas DESC
LIMIT 20;
```

**Definición**: para modelos con **≥ 10.000 descargas** (relevantes), el ratio
likes/descargas. Mide **aprobación/calidad percibida** relativa al uso.

**Interpretación**: un ratio alto indica modelos muy valorados por la comunidad
en relación con su uso (posible infraexplotación o calidad destacada); un ratio
bajo indica modelos muy descargados pero poco "gustados" (uso utilitario, o
modelos populares por defecto). Es un proxy de **reputación** del modelo.

**Uso en dashboard**: top-20 "modelos con mejor ratio likes/descargas".

---

## KPI 9 — Concentración del mercado (top-10 %)

**Vista**: `vw_kpi_concentracion`

```sql
WITH latest AS (
  SELECT 
         model_id, downloads,
         ROW_NUMBER() OVER (ORDER BY downloads DESC) AS rn
  FROM pf.gold.fact_metricas_diarias
  WHERE fecha_id = (SELECT MAX(fecha_id) FROM pf.gold.fact_metricas_diarias)
)
SELECT 
    SUM(CASE WHEN rn <= 10 THEN downloads ELSE 0 END) / SUM(downloads) * 100 AS pct_top10
FROM latest;
```

**Definición**: % de las descargas totales (snapshot más reciente) que concentran
los **10 modelos más descargados**.

**Interpretación**: mide la **concentración/desigualdad** del mercado. Un valor
alto (p. ej. >40%) indica un ecosistema dominado por pocos modelos estrella
(long-tail muy larga); un valor bajo indica distribución más uniforme. Es el
equivalente a un índice de concentración (tipo CR10) para el catálogo de IA.

**Uso en dashboard**: métrica de cabecera (`concentracion_top10_pct`).

---

## KPI 10 — Calidad del pipeline (data quality)

**Vista**: `vw_kpi_calidad`

```sql
SELECT
  (SELECT COUNT(*) FROM pf.gold.fact_metricas_diarias) AS filas_fact,
  (SELECT COUNT(*) FROM pf.silver.modelos WHERE has_metadata = TRUE) AS silver_con_payload,
  (SELECT COUNT(*) FROM pf.gold.dim_modelo_scd2 WHERE is_current = TRUE) AS modelos_vigentes,
  (SELECT COUNT(*) FROM pf.gold.dim_modelo_scd2)
    - (SELECT COUNT(*) FROM pf.gold.dim_modelo_scd2 WHERE is_current = TRUE) AS versiones_historicas;
```

**Definición**: métricas operativas del propio pipeline:
- `filas_fact`: filas en la tabla de hechos.
- `silver_con_payload`: modelos Silver con payload JSON íntegro (cobertura de
  metadatos).
- `modelos_vigentes`: modelos con versión SCD2 vigente.
- `versiones_historicas`: versiones cerradas del SCD2 (cuántos cambios de
  atributos se han registrado).

**Interpretación**: es el **KPI de salud del dato**. `versiones_historicas` alto
indica que el SCD2 está capturando evolución real de atributos; `silver_con_payload`
cercano al total indica buena cobertura de metadatos. Sirve para monitorizar que
el pipeline no pierde información entre capas.

**Uso en dashboard**: panel de control de calidad / operaciones.

---

## Resumen ejecutivo

| # | KPI | Pregunta que responde | Dimensión principal |
|---|---|---|---|
| 1 | Modelos nuevos/día | ¿A qué ritmo se innova? | Tiempo (creación) |
| 2 | Descargas acumuladas + delta | ¿Cuánto se usa el ecosistema y cómo fluye? | Tiempo (snapshot) |
| 3 | Ranking por tarea | ¿Qué tareas dominan la demanda? | Tarea |
| 4 | Top organizaciones | ¿Quiénes lideran el ecosistema? | Organización |
| 5 | Distribución por licencia | ¿Qué licencias predominan? | Licencia |
| 6 | % open-weight en nuevos | ¿La comunidad se vuelve más abierta? | Licencia + tiempo |
| 7 | Adopción semanal | ¿Crece por profundidad o amplitud? | Tiempo (semana) |
| 8 | Ratio likes/descargas | ¿Qué modelos son los más valorados? | Modelo |
| 9 | Concentración top-10 | ¿Cuán concentrado está el mercado? | Modelo (snapshot) |
| 10 | Calidad del pipeline | ¿El dato es confiable? | Operaciones |

Todas las vistas están en `pf.semantic` y son las **únicas** fuentes permitidas
para el dashboard.