# Saber 11: Pipeline Medallon (PySpark + Delta + ML + LLM)

Proyecto de Big Data - Fundacion Universitaria Konrad Lorenz.

## Objetivo
Construir un pipeline para estimar el puntaje esperado de cada colegio usando el contexto socioeconómico de sus estudiantes y comparar ese valor con el puntaje real para:
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

## Inicio rapido local
```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pytest -q
python main.py --layer silver
python main.py --layer gold
python main.py --layer ml
python main.py --layer llm
```

## Capas del pipeline
- Bronce: consulta la API de Socrata y valida el contrato antes de escribir.
- Plata: normaliza variables, filtra colegios oficiales, deduplica y genera el indice socioeconomico.
- Oro: crea el esquema estrella con dimensiones y hechos.
- ML: compara modelos de base y valida el flujo con datos de ejemplo locales.
- LLM: revisa resultados ya calculados y escribe una explicacion estructurada.

## Local execution notes
- En Windows puede requerirse `HADOOP_HOME=C:\hadoop` y `winutils.exe` en `C:\hadoop\bin` para que Spark funcione.
- Si no se define `SABER11_DATASET_ID`, el pipeline usa un dataset sintético para validar el flujo localmente.

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
