from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.silver.normalization import build_student_context, normalize_text, quarantine_invalid_rows


def test_normalize_text_removes_accents_and_upcases() -> None:
    assert normalize_text("café rural") == "CAFE RURAL"


def test_build_student_context_creates_index_and_standard_columns() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test_silver").config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
    try:
        df = spark.createDataFrame(
            [
                {
                    "_periodo": "20194",
                    "estu_consecutivo": "A1",
                    "cole_cod_dane_establecimiento": "110001",
                    "cole_naturaleza": "OFICIAL",
                    "cole_area_ubicacion": "URBANA",
                    "cole_jornada": "MANANA",
                    "cole_cod_depto_ubicacion": "11",
                    "cole_cod_mcpio_ubicacion": "001",
                    "fami_estratovivienda": "3",
                    "fami_educacionpadre": "BACHILLER",
                    "fami_educacionmadre": "BACHILLER",
                    "fami_tieneinternet": "1",
                    "fami_tienecomputador": "1",
                    "punt_global": 320,
                }
            ]
        )
        result = build_student_context(df)
        rows = result.collect()
        assert rows[0]["periodo"] == "20194"
        assert rows[0]["colegio_id"] == "110001"
        assert rows[0]["indice_socioeconomico"] >= 0.5
        assert rows[0]["municipio_id"] == "11-001"
    finally:
        spark.stop()


def test_quarantine_invalid_rows_separates_bad_records() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test_silver_quarantine").config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
    try:
        df = spark.createDataFrame(
            [
                {
                    "periodo": "20194",
                    "estu_consecutivo": "A1",
                    "colegio_id": "110001",
                    "punt_global": 320,
                    "fami_estratovivienda": 3,
                    "fami_tieneinternet": 1,
                    "fami_tienecomputador": 1,
                    "indice_socioeconomico": 0.7,
                },
                {
                    "periodo": "20194",
                    "estu_consecutivo": "",
                    "colegio_id": "110002",
                    "punt_global": None,
                    "fami_estratovivienda": 2,
                    "fami_tieneinternet": 0,
                    "fami_tienecomputador": 0,
                    "indice_socioeconomico": 0.2,
                },
            ]
        )
        valid, invalid = quarantine_invalid_rows(df)
        assert valid.count() == 1
        assert invalid.count() == 1
    finally:
        spark.stop()
