---
mode: agent
description: Fase 3: capa Oro (esquema estrella)
---
Sigue `.github/copilot-instructions.md` y `docs/PLAN.md`.

## Tarea
Implementa en Spark SQL las tablas de `pipelines/gold/*.sql`: dim_colegio, dim_municipio, dim_periodo, dim_segmento y fact_colegio_periodo
(agregado por colegio y periodo con porcentajes de contexto y n_estudiantes). Deja las columnas de modelo (puntaje_esperado, residuo, z_score, categoria, segmento_id) nulas hasta la fase de ML.
## Criterios de aceptacion
- Idempotente (INSERT OVERWRITE / MERGE). - Sin duplicados por (colegio_id, periodo_id).
