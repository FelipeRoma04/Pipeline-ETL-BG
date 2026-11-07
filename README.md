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
UNITY_CATALOG=saber11
UNITY_SCHEMA=analytics
```

## Local execution notes
- En Windows puede requerirse `HADOOP_HOME=C:\hadoop` y `winutils.exe` en `C:\hadoop\bin` para que Spark funcione.
- Si `SABER11_DATASET_ID` no existe, el pipeline usa un dataset sintetico para validar el flujo localmente.
- El flujo se puede lanzar con `--environment local` o `--environment databricks` segun el destino.

## Configuracion para Databricks + Unity Catalog
Este proyecto ya tiene un switch de ambiente para Databricks.

### Variables de entorno recomendadas
```bash
ENVIRONMENT=databricks
USE_FALLBACK=false
SABER11_DATASET_ID=kgxf-xxbe
SOCRATA_APP_TOKEN=<secret>
UNITY_CATALOG=saber11
UNITY_SCHEMA=analytics
```

### Crear catalogo y esquema en Unity Catalog
```sql
CREATE CATALOG IF NOT EXISTS saber11;
CREATE SCHEMA IF NOT EXISTS saber11.analytics;
```

### Secret Scope en Databricks
```bash
databricks secrets create-scope --scope saber11 --initial-manage-principal users
databricks secrets put --scope saber11 --key socrata_token --value "<tu-token-real>"
```

### Notebook de Databricks
```python
import os

os.environ["ENVIRONMENT"] = "databricks"
os.environ["USE_FALLBACK"] = "false"
os.environ["SOCRATA_APP_TOKEN"] = dbutils.secrets.get(scope="saber11", key="socrata_token")
os.environ["SABER11_DATASET_ID"] = "kgxf-xxbe"
os.environ["SOCRATA_BASE_URL"] = "https://www.datos.gov.co/resource"
os.environ["UNITY_CATALOG"] = "saber11"
os.environ["UNITY_SCHEMA"] = "analytics"

print("Configuracion de Databricks aplicada.")
```

### Validacion real con 1 periodo desde Databricks
La validacion real debe empezar por 1 solo periodo para confirmar fuente, schema y contrato antes de cargar todo el dataset.

```python
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer bronze --periodos 20194
```

Tambien se puede usar el script helper incluido:

```bash
python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/scripts/validate_databricks_period.py
```

### Ejecutar capas siguientes
```python
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer silver
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer gold
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer ml
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer llm
```

### Ejecutar todo el pipeline
```python
!python /Workspace/Repos/<usuario>/Pipeline-ETL-BG/main.py --environment databricks --no-fallback --layer all
```

### Verificar tablas creadas
```sql
SHOW TABLES IN saber11.analytics;
SELECT * FROM saber11.analytics.bronze_raw LIMIT 10;
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

## Nota de ejecucion real
Este repo se puede ejecutar de manera local sin credenciales externas. Para ejecucion real en Databricks, hace falta:
- token real de la API de Socrata,
- `dataset_id` correcto,
- catalogo y esquema creados en Unity Catalog,
- cluster Databricks con permisos de escritura sobre UC,
- validacion inicial de 1 periodo antes de cargar todo el dataset.
