from __future__ import annotations

import re
import unicodedata
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def normalize_text(value: Any) -> str:
    """Normaliza cadenas a mayusculas, sin tildes y sin espacios extras."""
    if value is None:
        return ""
    cleaned = str(value).strip()
    cleaned = unicodedata.normalize("NFKD", cleaned)
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = cleaned.upper()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def as_bool(value: Any) -> bool:
    """Convierte valores tipicos de SI/NO/1/0 a booleanos."""
    if value is None:
        return False
    text = normalize_text(value)
    return text in {"1", "SI", "S", "TRUE", "Y", "YES", "VERDADERO"}


def safe_float(value: Any, default: float = 0.0) -> float:
    """Convierte un valor a float de forma segura."""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_categorical(value: Any, alias_map: dict[str, str] | None = None) -> str:
    """Normaliza una categoria usando alias y texto limpio."""
    normalized = normalize_text(value)
    if alias_map:
        normalized = alias_map.get(normalized, normalized)
    return normalized


def build_student_context(df: DataFrame) -> DataFrame:
    """Normaliza y tipa las columnas relevantes de la capa Plata."""
    alias_maps = {
        "fami_estratovivienda": {"0": "0", "1": "1", "2": "2", "3": "3", "4": "4", "5": "5", "6": "6"},
        "fami_educacionpadre": {
            "NINGUNA": "NINGUNA",
            "PRIMARIA": "PRIMARIA",
            "BACHILLER": "BACHILLER",
            "TÉCNICO": "TECNICO",
            "UNIVERSITARIA": "UNIVERSITARIA",
            "POSTGRADO": "POSTGRADO",
        },
        "fami_educacionmadre": {
            "NINGUNA": "NINGUNA",
            "PRIMARIA": "PRIMARIA",
            "BACHILLER": "BACHILLER",
            "TÉCNICO": "TECNICO",
            "UNIVERSITARIA": "UNIVERSITARIA",
            "POSTGRADO": "POSTGRADO",
        },
        "cole_jornada": {"MAÑANA": "MANANA", "TARDE": "TARDE", "NOCHE": "NOCHE", "COMPLETA": "COMPLETA"},
        "cole_area_ubicacion": {"URBANA": "URBANA", "RURAL": "RURAL"},
        "cole_naturaleza": {"OFICIAL": "OFICIAL", "NO OFICIAL": "NO OFICIAL", "NO OFICIAL ": "NO OFICIAL"},
    }

    normalized = df.select(
        F.col("_periodo").alias("periodo"),
        F.col("estu_consecutivo").cast("string").alias("estu_consecutivo"),
        F.col("cole_cod_dane_establecimiento").cast("string").alias("colegio_id"),
        F.col("cole_cod_depto_ubicacion").cast("string").alias("cod_depto"),
        F.col("cole_cod_mcpio_ubicacion").cast("string").alias("cod_mpio"),
        F.col("cole_naturaleza").alias("cole_naturaleza_raw"),
        F.col("cole_area_ubicacion").alias("cole_area_ubicacion_raw"),
        F.col("cole_jornada").alias("cole_jornada_raw"),
        F.col("fami_estratovivienda").cast("string").alias("fami_estratovivienda_raw"),
        F.col("fami_educacionpadre").cast("string").alias("fami_educacionpadre_raw"),
        F.col("fami_educacionmadre").cast("string").alias("fami_educacionmadre_raw"),
        F.col("fami_tieneinternet").cast("string").alias("fami_tieneinternet_raw"),
        F.col("fami_tienecomputador").cast("string").alias("fami_tienecomputador_raw"),
        F.col("punt_global").cast("double").alias("punt_global"),
    )

    for col_name, alias_map in alias_maps.items():
        normalized = normalized.withColumn(
            col_name,
            F.when(F.col(col_name + "_raw").isNull(), F.lit(""))
            .otherwise(F.udf(lambda v: normalize_categorical(v, alias_map), "string")(F.col(col_name + "_raw"))),
        )

    normalized = normalized.withColumn(
        "es_oficial",
        F.lower(F.col("cole_naturaleza")).isin(["oficial"]),
    )
    normalized = normalized.withColumn("fami_tieneinternet", F.udf(lambda v: 1.0 if as_bool(v) else 0.0, "double")(F.col("fami_tieneinternet_raw")))
    normalized = normalized.withColumn("fami_tienecomputador", F.udf(lambda v: 1.0 if as_bool(v) else 0.0, "double")(F.col("fami_tienecomputador_raw")))
    normalized = normalized.withColumn("fami_estratovivienda", F.udf(lambda v: safe_float(v, 0.0), "double")(F.col("fami_estratovivienda_raw")).cast("double"))
    normalized = normalized.withColumn("municipio_id", F.concat(F.col("cod_depto"), F.lit("-"), F.col("cod_mpio")))

    normalized = normalized.withColumn(
        "indice_socioeconomico",
        (
            F.when(F.col("fami_tieneinternet") > 0, F.lit(0.2)).otherwise(F.lit(0.0))
            + F.when(F.col("fami_tienecomputador") > 0, F.lit(0.2)).otherwise(F.lit(0.0))
            + F.when(F.col("fami_estratovivienda").between(1, 3), F.lit(0.25)).otherwise(F.lit(0.1))
            + F.when(F.col("cole_area_ubicacion") == "URBANA", F.lit(0.15)).otherwise(F.lit(0.05))
            + F.when(F.col("es_oficial"), F.lit(0.2)).otherwise(F.lit(0.0))
        ).cast("double"),
    )

    normalized = normalized.withColumn("cole_area_ubicacion", F.udf(lambda v: normalize_categorical(v, {"URBANA": "URBANA", "RURAL": "RURAL"}), "string")(F.col("cole_area_ubicacion_raw")))
    normalized = normalized.withColumn("cole_jornada", F.udf(lambda v: normalize_categorical(v, {"MANANA": "MANANA", "TARDE": "TARDE", "NOCHE": "NOCHE", "COMPLETA": "COMPLETA"}), "string")(F.col("cole_jornada_raw")))

    return normalized.drop(
        "cole_naturaleza_raw",
        "cole_area_ubicacion_raw",
        "cole_jornada_raw",
        "fami_estratovivienda_raw",
        "fami_educacionpadre_raw",
        "fami_educacionmadre_raw",
        "fami_tieneinternet_raw",
        "fami_tienecomputador_raw",
    )


def quarantine_invalid_rows(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa filas validas de filas rechazadas por registros vacios o inconsistentes."""
    invalid_condition = (
        F.col("periodo").isNull()
        | (F.trim(F.col("periodo").cast("string")) == "")
        | F.col("estu_consecutivo").isNull()
        | (F.trim(F.col("estu_consecutivo").cast("string")) == "")
        | F.col("colegio_id").isNull()
        | (F.trim(F.col("colegio_id").cast("string")) == "")
        | F.col("punt_global").isNull()
    )
    invalid = df.filter(invalid_condition)
    valid = df.exceptAll(invalid)
    invalid = invalid.withColumn("motivo_rechazo", F.lit("faltan campos obligatorios o puntaje nulo"))
    return valid, invalid


def build_divipola_dimension(df: DataFrame) -> DataFrame:
    """Genera una tabla de municipios a partir de los codigos presentes en la capa Plata."""
    return (
        df.select(
            F.col("cod_depto").cast("string").alias("cod_depto"),
            F.col("cod_mpio").cast("string").alias("cod_mpio"),
            F.col("municipio_id").alias("municipio_id"),
        )
        .dropDuplicates()
        .withColumn("departamento", F.lit("Departamento no informado"))
        .withColumn("municipio", F.lit("Municipio no informado"))
    )


def ensure_spark_session() -> SparkSession:
    """Factory para una sesion local con Delta habilitado."""
    import os

    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder.appName("saber11_silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
