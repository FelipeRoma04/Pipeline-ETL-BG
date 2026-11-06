"""Carga la capa Bronce del dataset de Saber 11 desde la API Socrata."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.contract_loader import load_contract
from config.settings import BRONZE_PATH, PERIODOS, SABER11_DATASET_ID, SOCRATA_BASE_URL, SOCRATA_APP_TOKEN

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea la SparkSession con Delta habilitado para esta capa."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("saber11_bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.local.hostname", "localhost")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def make_headers() -> dict[str, str]:
    """Arma los headers de la API Socrata."""
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN
    return headers


def fetch_rows_for_period(period: str, limit: int | None = None, page_size: int = 50000) -> list[dict[str, Any]]:
    """Consulta todos los registros de un periodo, paginando la API de Socrata."""
    url = f"{SOCRATA_BASE_URL}/{SABER11_DATASET_ID}.json"
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        params: dict[str, Any] = {
            "$where": f"periodo='{period}'",
            "$limit": min(page_size, limit) if limit is not None else page_size,
            "$offset": offset,
        }
        response = requests.get(url, params=params, headers=make_headers(), timeout=120)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        rows.extend(batch)
        if limit is not None and len(rows) >= limit:
            return rows[:limit]
        if len(batch) < params["$limit"]:
            break
        offset += params["$limit"]

    return rows


def normalize_record(record: dict[str, Any], period: str) -> dict[str, Any]:
    """Añade metadatos requeridos a cada fila cruda."""
    normalized = dict(record)
    normalized["_periodo"] = str(period)
    normalized["_ingest_ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized["_fuente"] = "socrata_datos_gov_co"
    return normalized


def to_raw_df(spark: SparkSession, rows: list[dict[str, Any]], period: str) -> DataFrame:
    """Convierte el lote de filas en un DataFrame escalable con columnas raw."""
    if not rows:
        raise ValueError(f"No se obtuvieron filas para el periodo {period}.")

    raw_rows = [normalize_record(row, period) for row in rows]
    df = spark.createDataFrame(raw_rows)
    return df


def validate_contract(df: DataFrame, contract: dict[str, Any]) -> None:
    """Valida el DataFrame contra el contrato YAML antes de escribir Bronce."""
    expected_headers = contract.get("expected_headers", [])
    required_cols = contract.get("required_cols", [])
    max_invalid_ratio = float(contract.get("max_invalid_ratio", 0.0))

    missing_expected = [col for col in expected_headers if col not in df.columns]
    if missing_expected:
        raise ValueError(f"Faltan columnas esperadas en el DataFrame: {missing_expected}")

    missing_required = [col for col in required_cols if col not in df.columns]
    if missing_required:
        raise ValueError(f"Faltan columnas requeridas: {missing_required}")

    total_rows = df.count()
    if total_rows == 0:
        raise ValueError("El DataFrame final no tiene filas para validar.")

    invalid_exprs = []
    for col in required_cols:
        invalid_exprs.append(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == ""))

    invalid_rows = df.filter(F.coalesce(*invalid_exprs)) if invalid_exprs else df.limit(0)
    invalid_ratio = invalid_rows.count() / total_rows

    if invalid_ratio > max_invalid_ratio:
        raise ValueError(
            f"El ratio de filas inválidas ({invalid_ratio:.2%}) excede max_invalid_ratio={max_invalid_ratio:.2%}."
        )

    logger.info("Validación de contrato OK: %s filas, ratio inválido %.2f%%", total_rows, invalid_ratio * 100)


def write_delta_partition(df: DataFrame, period: str) -> str:
    """Escribe el periodo en Delta con partición por periodo y reemplazo idempotente."""
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    output_path = str(BRONZE_PATH)
    df.write.format("delta").mode("overwrite").option("replaceWhere", f"_periodo = '{period}'").partitionBy("_periodo").save(output_path)
    logger.info("Bronce escrito en %s para periodo %s", output_path, period)
    return output_path


def run_bronze(periodos: list[str] | None = None, limit: int | None = None) -> list[str]:
    """Runner principal de la capa Bronce."""
    spark = create_spark_session()
    try:
        selected_periodos = periodos or PERIODOS
        contract = load_contract("saber11_contract")
        results: list[str] = []

        for period in selected_periodos:
            logger.info("Consultando periodo %s desde la API", period)
            rows = fetch_rows_for_period(period, limit=limit)
            logger.info("Periodo %s: %s filas recuperadas", period, len(rows))

            df = to_raw_df(spark, rows, period)
            validate_contract(df, contract)
            output_path = write_delta_partition(df, period)
            results.append(output_path)

        return results
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejecuta la capa Bronce del pipeline Saber 11")
    parser.add_argument("--periodos", nargs="*", default=None, help="Periodos a procesar, por ejemplo 20194 20224")
    parser.add_argument("--limit", type=int, default=None, help="Limite de filas por periodo para pruebas pequeñas")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    periodos = parse_args().periodos
    run_bronze(periodos=periodos, limit=parse_args().limit)
