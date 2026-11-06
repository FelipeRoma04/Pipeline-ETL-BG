---
mode: agent
description: Fase 6: LLM para reportes explicativos por colegio
---
Sigue `.github/copilot-instructions.md` y `docs/PLAN.md`.

## Tarea
En `llm/reportes.py`: para cada colegio prioritario/modelo, arma un payload JSON con cluster, residuo, z-score y variables de contexto clave (todo desde Oro) y pide al LLM un reporte corto con recomendacion. Prompt versionado, salida JSON, sin inventar cifras. Escribe en `gold.fact_explicacion`. Opcional `llm/rag_metadatos.py`: preguntas sobre el diccionario de variables.
## Criterios de aceptacion
- El texto solo usa cifras del payload. - Muestra de 20 reportes revisada a mano.
