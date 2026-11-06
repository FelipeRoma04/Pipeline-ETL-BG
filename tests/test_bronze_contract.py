from __future__ import annotations

from pyspark.sql import SparkSession

from config.contract_loader import load_contract
from pipelines.bronze.run_bronze import validate_contract, to_raw_df


def test_bronze_contract_accepts_valid_rows() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test_bronze_contract").config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
    try:
        contract = load_contract("saber11_contract")
        rows = [
            {
                "periodo": "20194",
                "estu_consecutivo": "A1",
                "cole_cod_dane_establecimiento": "110001",
                "cole_naturaleza": "OFICIAL",
                "cole_area_ubicacion": "URBANA",
                "cole_jornada": "MAÑANA",
                "cole_cod_depto_ubicacion": "11",
                "cole_cod_mcpio_ubicacion": "001",
                "fami_estratovivienda": "3",
                "fami_educacionpadre": "BACHILLER",
                "fami_educacionmadre": "BACHILLER",
                "fami_tieneinternet": "1",
                "fami_tienecomputador": "1",
                "punt_global": "320",
            }
        ]
        df = to_raw_df(spark, rows, "20194")
        validate_contract(df, contract)
        assert df.count() == 1
    finally:
        spark.stop()


def test_bronze_contract_rejects_invalid_ratio() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test_bronze_contract_invalid").config(
        "spark.sql.shuffle.partitions", "1"
    ).getOrCreate()
    try:
        contract = load_contract("saber11_contract")
        rows = [
            {
                "periodo": "20194",
                "estu_consecutivo": "A1",
                "cole_cod_dane_establecimiento": "110001",
                "cole_naturaleza": "OFICIAL",
                "cole_area_ubicacion": "URBANA",
                "cole_jornada": "MAÑANA",
                "cole_cod_depto_ubicacion": "11",
                "cole_cod_mcpio_ubicacion": "001",
                "fami_estratovivienda": "3",
                "fami_educacionpadre": "BACHILLER",
                "fami_educacionmadre": "BACHILLER",
                "fami_tieneinternet": "1",
                "fami_tienecomputador": "1",
                "punt_global": "320",
            },
            {
                "periodo": "20194",
                "estu_consecutivo": "",
                "cole_cod_dane_establecimiento": "110002",
                "cole_naturaleza": "OFICIAL",
                "cole_area_ubicacion": "URBANA",
                "cole_jornada": "MAÑANA",
                "cole_cod_depto_ubicacion": "11",
                "cole_cod_mcpio_ubicacion": "002",
                "fami_estratovivienda": "3",
                "fami_educacionpadre": "BACHILLER",
                "fami_educacionmadre": "BACHILLER",
                "fami_tieneinternet": "1",
                "fami_tienecomputador": "1",
                "punt_global": "310",
            },
        ]
        df = to_raw_df(spark, rows, "20194")
        try:
            validate_contract(df, contract)
            assert False, "Se esperaba una excepción por ratio de inválidos"
        except ValueError:
            pass
    finally:
        spark.stop()
