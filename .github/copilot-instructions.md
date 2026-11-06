# Instrucciones del proyecto para GitHub Copilot

## Contexto
Proyecto de Big Data (Konrad Lorenz). Pipeline Medallon (Bronce/Plata/Oro) en **PySpark + Delta Lake**
sobre Resultados Saber 11 (ICFES), periodos **2019-4 y 2022-4** (~2,16 M registros).
Objetivo: estimar el puntaje esperado de cada colegio segun el contexto socioeconomico de sus estudiantes
y comparar con el real (residuo) para: (1) priorizar colegios con apoyo, (2) identificar colegios modelo,
(3) segmentar colegios con K-Means. Ademas: MLP (FNN), RNN (LSTM/GRU) y LLM para reportes explicativos.

## Stack y convenciones
- Python 3.11, PySpark DataFrame API. **No usar pandas sobre el dataset completo** (solo muestras o resultados agregados).
- Delta Lake para Bronce/Plata/Oro. Rutas desde `config/settings.py`. Secretos solo por `.env`.
- snake_case, type hints, docstrings en espanol, `logging` (nunca `print`).
- Un modulo por entidad en Plata (`run_<entidad>()`), y un runner que las llama en orden de dependencia.
- Mapeos de columnas en `config/schemas/columns_mappings.py` (uno por periodo).
- Contratos YAML por fuente en `config/contracts/bronze/` y se validan antes de escribir Bronce.

## Reglas por capa
**Bronce:** datos crudos tal cual (todo string) + columnas `_periodo`, `_ingest_ts`, `_fuente`. Particion por periodo.
Validar contra contrato; si `max_invalid_ratio` se excede, la ejecucion falla. Registrar conteo de filas por periodo.
**Plata:** castear tipos, normalizar categorias (estrato, educacion padres, internet, computador, zona, jornada),
normalizar texto (mayusculas, sin tildes), cruzar con DIVIPOLA, filtrar colegios oficiales, deduplicar.
Filas rechazadas van a `data/quarantine` con columna `motivo_rechazo`. Calcular indice socioeconomico.
**Oro:** esquema estrella (`dim_*`, `fact_*`) definido en `pipelines/gold/*.sql`. Idempotente.

## Idempotencia (obligatorio)
Reejecutar cualquier capa no debe duplicar datos: usar `replaceWhere` por periodo o `MERGE INTO`.
Nunca `TRUNCATE`+`INSERT` sin transaccion.

## Reglas de ML
- Features de regresion = **solo contexto** (nada que identifique al colegio ni el puntaje de las areas).
- Evitar fuga de datos: split **agrupado por colegio**; ademas probar entrenar 2019-4 y evaluar 2022-4.
- Residuo por colegio = promedio de residuos de sus estudiantes, con ajuste (shrinkage) o minimo de evaluados.
- Anomalias: z-score del residuo (<= -1.5 prioritario, >= +1.5 modelo; umbrales configurables).
- K-Means: escoger k con silhouette y codo; estandarizar features.
- MLP: compararlo contra Random Forest/GBT (RMSE, MAE, R2) y reportar honestamente.
- RNN (LSTM/GRU): solo con secuencias por colegio (varios periodos). Si hay solo 2 periodos, marcarlo como prueba de concepto.
- Guardar resultados en `gold.fact_prediccion` con `modelo` y `version`.

## Reglas de LLM
El LLM **no calcula**: recibe cifras ya calculadas del Oro y redacta. Salida estructurada (JSON), prompt versionado,
solo usa cifras entregadas, y se revisa una muestra manualmente. Resultado a `gold.fact_explicacion`.

## Pruebas
`pytest` en `tests/`: contratos, funciones de normalizacion, y conteos entre capas (Bronce >= Plata + cuarentena).
