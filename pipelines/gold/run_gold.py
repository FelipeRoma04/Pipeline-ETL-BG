from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import GOLD_PATH, SILVER_PATH
from pipelines.silver.run_silver import run_silver

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea la SparkSession con soporte Delta para la capa Oro."""
    import os

    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    path = os.environ.get("PATH", "")
    winutils_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    if winutils_bin not in path:
        os.environ["PATH"] = path + (";" if path else "") + winutils_bin

    builder = (
        SparkSession.builder.appName("saber11_gold")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_silver_estudiantes(spark: SparkSession) -> DataFrame:
    """Carga la tabla validada de Plata; si no existe, la genera."""
    silver_dir = Path(SILVER_PATH)
    estudiantes_path = silver_dir / "estudiantes"
    if estudiantes_path.exists():
        return spark.read.format("delta").load(str(estudiantes_path))
    run_silver()
    return spark.read.format("delta").load(str(estudiantes_path))


def build_dim_colegio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("colegio_id").cast("string").alias("colegio_id"),
            F.col("colegio_id").alias("colegio_nombre"),
            F.col("municipio_id"),
            F.col("es_oficial"),
        )
        .dropDuplicates()
    )


def build_dim_municipio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("municipio_id").alias("municipio_id"),
            F.col("cod_depto").alias("cod_depto"),
            F.col("cod_mpio").alias("cod_mpio"),
        )
        .dropDuplicates()
    )


def build_dim_periodo(df: DataFrame) -> DataFrame:
    return df.select(F.col("periodo").alias("periodo_id")).dropDuplicates().orderBy(F.col("periodo"))


def build_fact_colegio_periodo(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("periodo", "colegio_id")
        .agg(
            F.avg("punt_global").alias("puntaje_promedio"),
            F.avg("indice_socioeconomico").alias("indice_socioeconomico_promedio"),
            F.count("estu_consecutivo").alias("num_estudiantes"),
        )
        .withColumnRenamed("periodo", "periodo_id")
    )


def run_gold() -> dict[str, str]:
    """Construye el esquema estrella de Oro desde la capa Plata."""
    spark = create_spark_session()
    try:
        students = load_silver_estudiantes(spark)
        GOLD_PATH.mkdir(parents=True, exist_ok=True)

        dim_colegio = build_dim_colegio(students)
        dim_municipio = build_dim_municipio(students)
        dim_periodo = build_dim_periodo(students)
        fact = build_fact_colegio_periodo(students)

        dim_colegio.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_colegio"))
        dim_municipio.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_municipio"))
        dim_periodo.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_periodo"))
        fact.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "fact_colegio_periodo"))

        logger.info("Capa Oro completada en %s", GOLD_PATH)
        return {
            "dim_colegio": str(GOLD_PATH / "dim_colegio"),
            "dim_municipio": str(GOLD_PATH / "dim_municipio"),
            "dim_periodo": str(GOLD_PATH / "dim_periodo"),
            "fact_colegio_periodo": str(GOLD_PATH / "fact_colegio_periodo"),
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    run_gold()
