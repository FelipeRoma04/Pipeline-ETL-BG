---
mode: agent
description: Fase 1: ingesta Bronce (API Socrata -> Delta)
---
Sigue `.github/copilot-instructions.md` y `docs/PLAN.md`.

## Tarea
Implementa la capa Bronce:
1. `ingestion/api_client.py`: cliente Socrata con paginacion ($limit/$offset), filtro por `periodo`, reintentos y app token opcional.
2. `ingestion/normalizer.py`: normaliza encabezados a snake_case sin tildes.
3. `ingestion/validators.py`: normaliza nulos tipo 'nan'/'null'/'', elimina filas sin columnas obligatorias y valida numericos; falla si supera `max_invalid_ratio` del contrato.
4. `ingestion/writer.py`: escribe Delta particionado por periodo con `replaceWhere` (idempotente) y columnas de auditoria.
5. `pipelines/bronze/bronze_pipeline.py` + `router.py`: elige contrato por fuente (saber11, divipola).
## Criterios de aceptacion
- Reejecutar no duplica filas. - Log con conteo por periodo. - Tests en `tests/test_bronze.py`.
