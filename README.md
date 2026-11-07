# Saber 11: Pipeline Medallon (PySpark + Delta + ML + LLM)

Proyecto de Big Data - Fundacion Universitaria Konrad Lorenz.

## Objetivo
Construir un pipeline para estimar el puntaje esperado de cada colegio usando el contexto socioeconomico de sus estudiantes y comparar ese valor con el puntaje real para:
- priorizar colegios con apoyo,
- identificar colegios modelo,
- segmentar colegios con K-Means,
- evaluar modelos de regresion, redes neuronales y explicacion generada por LLM.

## Stack
- Python 3.11+
- PySpark + Delta Lake
- scikit-learn
- PyTorch
- pytest
- Databricks + Unity Catalog (modo produccion)

## Arquitectura
- Bronce: ingesta datos crudos, agrega metadatos y valida contrato.
- Plata: normaliza tipos, categorias, deduplicacion, cuarentena y indice socioeconomico.
- Oro: esquema estrella con dimensiones y hechos.
- ML: compara modelos de contexto (regresion, RF, MLP, K-Means).
- LLM: interpreta resultados ya calculados y produce un resumen estructurado.

## Inicio rapido local
```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pytest -q
python main.py --layer bronze
python main.py --layer silver
python main.py --layer gold
python main.py --layer ml
python main.py --layer llm
```

## Modo local por defecto
El proyecto corre por defecto en modo local con fallback activo para que no falle si no hay dataset externo real.

```bash
ENVIRONMENT=local
USE_FALLBACK=true
SABER11_DATASET_ID=
SOCRATA_APP_TOKEN=
UNITY_CATALOG=workspace
```

## Local execution notes
- En Windows puede requerirse `HADOOP_HOME=C:\hadoop` y `winutils.exe` en `C:\hadoop\bin` para que Spark funcione.
- Si `SABER11_DATASET_ID` no existe, el pipeline usa un dataset sintetico para validar el flujo localmente.
- El flujo se puede lanzar con `--environment local` o `--environment databricks` segun el destino.

## Ejecucion en Databricks + Unity Catalog
El proyecto **detecta Databricks automaticamente** (`DATABRICKS_RUNTIME_VERSION`) y reusa la SparkSession del notebook. No hay que instalar nada: sklearn, requests y yaml ya vienen en el runtime. Las variables de entorno tienen defaults correctos; solo son opcionales para sobreescribir valores.

### Destino de datos (catalogo `workspace`)
- Un esquema por capa: `workspace.bronze`, `workspace.silver`, `workspace.gold`.
- Bronze: `workspace.bronze.saber11`, particionada por `_periodo`, escritura **idempotente por periodo** (`replaceWhere`) — re-ejecutar un periodo reemplaza su particion completa sin tocar los demas.
- Silver: `estudiantes`, `quarantine`, `divipola`. Gold: `dim_colegio`, `dim_municipio`, `dim_periodo`, `fact_colegio_periodo`, `fact_anomalia`.
- Salidas ML/LLM (JSON): `/Workspace/Users/<usuario>/saber11_data/gold/`.

### Variables de entorno (todas opcionales, defaults correctos)
```bash
ENVIRONMENT=databricks        # se autodetecta, no hace falta fijarlo
USE_FALLBACK=false            # en Databricks el default ya es false
SABER11_DATASET_ID=kgxf-xxbe
SOCRATA_APP_TOKEN=<opcional, solo evita rate limits>
UNITY_CATALOG=workspace
```

### Notebooks de ejecucion (en la carpeta `notebooks/` del usuario en el workspace)
- `01_run_pipeline_dbx` — pipeline completo con widgets: **periodos** (coma-separados) y **limit** (0 = sin limite).
- `02_bronze_dbx`, `03_silver_dbx`, `04_gold_dbx`, `05_ml_dbx`, `06_llm_dbx` — un notebook por capa para demostrar cada ejecucion por separado.

Cada notebook agrega este repo a `sys.path` y llama a los runners reales (`run_bronze`, `run_silver`, `run_gold`, `run_ml`, `run_llm`). El flujo local con `python main.py` sigue intacto para Windows.

> ⚠️ Bronze con `limit` bajo **encoge la particion** del periodo: usa `limit` solo para pruebas con periodos nuevos, no sobre datos ya cargados.

### Verificar tablas creadas
```sql
SELECT _periodo, COUNT(*) FROM workspace.bronze.saber11 GROUP BY _periodo;   -- 20194: 1.096.524 | 20224: 1.065.888
SELECT COUNT(*) FROM workspace.silver.estudiantes;                             -- 2.162.410 validas
SELECT COUNT(*) FROM workspace.gold.fact_colegio_periodo;                     -- 20.539 hechos colegio-periodo
```

## Regla de idempotencia
El proyecto evita duplicar datos usando `overwrite` y particiones o `MERGE` por periodo cuando aplica. No se debe usar `TRUNCATE + INSERT` sin control.

## Validacion
```bash
python -m pytest -q
```

## Checklist de progreso
- [x] Entorno local con Spark y dependencias
- [x] Capa Bronce validada con prueba de contrato
- [x] Capa Plata implementada localmente con normalizacion y cuarentena
- [x] Capa Oro implementada con lectura desde Plata y salida Delta
- [x] Capa ML implementada como baseline local
- [x] Capa LLM implementada como resumen estructurado
- [x] Orquestacion principal lista en `main.py`
- [x] Switch de ambiente local vs Databricks preparado
- [x] Unity Catalog + Databricks documentado en este README
- [x] Pipeline ejecutado end-to-end en Databricks (2,16 M filas en Bronze, ambos periodos)

## Nota de ejecucion real
Este repo se puede ejecutar de manera local sin credenciales externas. En Databricks ya corre sin pasos extra: los notebooks usan la SparkSession del workspace, el dataset publico de Socrata no requiere token (solo es opcional para evitar rate limits) y escriben en el catalogo `workspace`. Antes de cargar todo el dataset, valida 1 solo periodo (por ejemplo 20224).
