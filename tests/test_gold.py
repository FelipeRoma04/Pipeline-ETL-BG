from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.gold.run_gold import build_fact_anomalia, build_fact_colegio_periodo, build_dim_colegio, build_dim_municipio, build_dim_periodo


def test_gold_analytical_tables_are_generated() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test_gold_analytics").config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
    try:
        rows = [
            {
                "periodo": "20194",
                "colegio_id": "C001",
                "municipio_id": "11-001",
                "cod_depto": "11",
                "cod_mpio": "001",
                "estu_consecutivo": "A1",
                "punt_global": 320.0,
                "indice_socioeconomico": 0.8,
                "es_oficial": True,
                "cole_area_ubicacion": "URBANA",
                "cole_jornada": "MANANA",
            },
            {
                "periodo": "20194",
                "colegio_id": "C001",
                "municipio_id": "11-001",
                "cod_depto": "11",
                "cod_mpio": "001",
                "estu_consecutivo": "A2",
                "punt_global": 360.0,
                "indice_socioeconomico": 0.9,
                "es_oficial": True,
                "cole_area_ubicacion": "URBANA",
                "cole_jornada": "MANANA",
            },
            {
                "periodo": "20224",
                "colegio_id": "C002",
                "municipio_id": "11-002",
                "cod_depto": "11",
                "cod_mpio": "002",
                "estu_consecutivo": "B1",
                "punt_global": 300.0,
                "indice_socioeconomico": 0.6,
                "es_oficial": False,
                "cole_area_ubicacion": "RURAL",
                "cole_jornada": "TARDE",
            },
        ]
        df = spark.createDataFrame(rows)

        dim_colegio = build_dim_colegio(df)
        dim_municipio = build_dim_municipio(df)
        dim_periodo = build_dim_periodo(df)
        fact = build_fact_colegio_periodo(df)
        fact_anomalia = build_fact_anomalia(fact)

        assert dim_colegio.count() >= 2
        assert dim_municipio.count() >= 2
        assert dim_periodo.count() == 2
        assert fact.count() >= 2
        assert "zscore_residuo" in fact.columns
        assert "es_prioritario" in fact.columns
        assert "es_modelo" in fact.columns
        assert fact_anomalia.count() >= 1
    finally:
        spark.stop()
