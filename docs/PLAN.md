# Plan maestro: Saber 11 - Medallon + ML + Redes + LLM

## Arquitectura
```
API datos.gov.co (2019-4, 2022-4) + DIVIPOLA (DANE) + DataIcfes (metadatos)
   -> BRONCE (Delta crudo, particion por periodo, auditoria)
   -> PLATA  (tipado, normalizacion, DIVIPOLA, cuarentena, features)
   -> ORO    (estrella: dim_* / fact_*)
   -> ML (regresion, anomalias, K-Means) + MLP + LSTM/GRU -> fact_prediccion
   -> LLM (reportes) -> fact_explicacion
   -> Dashboard / informe
```

## Fases y checklist
- [ ] **F0** Entorno: repo, `.env`, Spark+Delta, `pytest` corre.
- [ ] **F1** Bronce (prompt `01_bronze`) + conteos ~2,16 M validados.
- [ ] **F1b** Diccionario de variables 2019 vs 2022 (`docs/data_dictionary.md`) + mapeos.
- [ ] **F2** Plata (prompt `02_silver`) + cuarentena + indice socioeconomico.
- [ ] **F3** Oro (prompt `03_gold`).
- [ ] **F4** ML (prompt `04_ml`): baseline, residuos, anomalias, K-Means.
- [ ] **F5** MLP y LSTM/GRU (prompt `05_redes_neuronales`).
- [ ] **F6** LLM (prompt `06_llm`).
- [ ] **F7** Dashboard, informe y presentacion.

## Decisiones a justificar en el informe
1. Por que Delta + PySpark (volumen, idempotencia, MERGE).
2. Que se descarto en Plata y por que (cuarentena).
3. Features solo de contexto para que el residuo mida al colegio.
4. Split agrupado por colegio y prueba 2019 -> 2022 (cambio por pandemia).
5. Comparacion honesta MLP vs arboles.
6. Limite de la RNN con 2 periodos y como se escala con mas periodos.
7. LLM solo narra, no calcula.

## Riesgos
- Nombres de columnas distintos entre periodos.
- Colegios con pocos estudiantes (residuo ruidoso).
- Pandemia 2019 vs 2022 (cambio de distribucion).
- Datos personales: trabajar solo con datos publicos y agregar a nivel colegio.
