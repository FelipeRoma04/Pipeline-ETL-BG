---
mode: agent
description: Fase 2: capa Plata (limpieza, tipado, DIVIPOLA, features)
---
Sigue `.github/copilot-instructions.md` y `docs/PLAN.md`.

## Tarea
Implementa la capa Plata con un modulo por entidad en `pipelines/silver/`:
`divipola.py`, `colegios.py`, `estudiantes.py` y `run_silver.py` (orden de dependencias).
- Aplicar el mapeo de columnas por periodo desde `config/schemas/columns_mappings.py`.
- Castear tipos, normalizar categorias, cruzar con DIVIPOLA, filtrar colegios oficiales, deduplicar.
- Calcular indice socioeconomico y banderas (estrato 1-2, sin internet, sin computador, padres baja escolaridad).
- Rechazados a `data/quarantine` con `motivo_rechazo`.
## Criterios de aceptacion
- Bronce >= Plata + cuarentena. - Idempotente. - Tests de normalizacion y de conteos.
