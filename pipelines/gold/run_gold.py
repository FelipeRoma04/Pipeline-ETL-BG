from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window, functions as F

from config.settings import GOLD_PATH, SILVER_PATH, get_uc_table_name, is_databricks
from pipelines.silver.run_silver import run_silver

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea la SparkSession con soporte Delta para la capa Oro."""
    import os
    import sys

    from delta import configure_spark_with_delta_pip

    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
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


def load_silver_estudiantes(spark: SparkSession, periodos: list[str] | None = None) -> DataFrame:
    """Carga la tabla validada de Plata local o desde Unity Catalog, filtrando periodos si aplica."""
    if is_databricks():
        try:
            df = spark.read.table(get_uc_table_name("silver", "estudiantes"))
            if periodos:
                df = df.filter(F.col("periodo").isin(periodos))
            return df
        except Exception:
            logger.warning("No fue posible leer la capa Plata desde Unity Catalog; se intenta generar la capa localmente.")

    silver_dir = Path(SILVER_PATH)
    estudiantes_path = silver_dir / "estudiantes"
    if estudiantes_path.exists():
        df = spark.read.format("delta").load(str(estudiantes_path))
        if periodos:
            df = df.filter(F.col("periodo").isin(periodos))
        return df
    run_silver(periodos=periodos)
    df = spark.read.format("delta").load(str(estudiantes_path))
    if periodos:
        df = df.filter(F.col("periodo").isin(periodos))
    return df


def build_dim_colegio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("colegio_id").cast("string").alias("colegio_id"),
            F.col("colegio_id").cast("string").alias("colegio_nombre"),
            F.col("municipio_id").cast("string").alias("municipio_id"),
            F.col("cod_depto").cast("string").alias("cod_depto"),
            F.col("cod_mpio").cast("string").alias("cod_mpio"),
            F.col("es_oficial").alias("es_oficial"),
            F.col("cole_area_ubicacion").alias("area_ubicacion"),
            F.col("cole_jornada").alias("jornada"),
        )
        .dropDuplicates()
    )


def build_dim_municipio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("municipio_id").cast("string").alias("municipio_id"),
            F.col("cod_depto").cast("string").alias("cod_depto"),
            F.col("cod_mpio").cast("string").alias("cod_mpio"),
            F.lit("Municipio no informado").alias("municipio_nombre"),
            F.lit("Departamento no informado").alias("departamento_nombre"),
        )
        .dropDuplicates()
    )


def build_dim_periodo(df: DataFrame) -> DataFrame:
    return df.select(F.col("periodo").cast("string").alias("periodo_id")).dropDuplicates().orderBy(F.col("periodo_id"))


def build_fact_colegio_periodo(df: DataFrame) -> DataFrame:
    periodo_window = Window.partitionBy("periodo")
    fact = (
        df.groupBy("periodo", "colegio_id")
        .agg(
            F.avg("punt_global").alias("puntaje_promedio"),
            F.avg("indice_socioeconomico").alias("indice_socioeconomico_promedio"),
            F.count("estu_consecutivo").alias("num_estudiantes"),
            F.min("es_oficial").alias("es_oficial"),
        )
        .withColumn("periodo_promedio", F.avg("puntaje_promedio").over(periodo_window))
        .withColumn("periodo_stddev", F.stddev("puntaje_promedio").over(periodo_window))
        .withColumn("residuo_promedio", F.col("puntaje_promedio") - F.col("periodo_promedio"))
        .withColumn(
            "zscore_residuo",
            F.when((F.col("periodo_stddev").isNull()) | (F.col("periodo_stddev") == 0), F.lit(0.0))
            .otherwise(F.col("residuo_promedio") / F.col("periodo_stddev")),
        )
    )

    return (
        fact.withColumn("es_prioritario", F.col("zscore_residuo") <= F.lit(-1.5))
        .withColumn("es_modelo", F.col("zscore_residuo") >= F.lit(1.5))
        .withColumnRenamed("periodo", "periodo_id")
    )


def build_fact_anomalia(df: DataFrame) -> DataFrame:
    """Tabla analitica de anomalias por colegio y periodo usando z-score del residuo."""
    return (
        df.select(
            F.col("periodo_id").alias("periodo"),
            F.col("colegio_id"),
            F.col("puntaje_promedio"),
            F.col("residuo_promedio"),
            F.col("zscore_residuo"),
            F.col("es_prioritario"),
            F.col("es_modelo"),
            F.col("num_estudiantes"),
        )
        .filter(F.col("zscore_residuo").isNotNull())
        .orderBy(F.col("zscore_residuo").desc())
    )


def run_gold(periodos: list[str] | None = None) -> dict[str, str]:
    """Construye el esquema estrella de Oro desde la capa Plata, incluyendo anomalias y metricas analiticas."""
    spark = create_spark_session()
    try:
        students = load_silver_estudiantes(spark, periodos=periodos)

        dim_colegio = build_dim_colegio(students)
        dim_municipio = build_dim_municipio(students)
        dim_periodo = build_dim_periodo(students)
        fact = build_fact_colegio_periodo(students)
        fact_anomalia = build_fact_anomalia(fact)

        if is_databricks():
            dim_colegio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(get_uc_table_name("gold", "dim_colegio"))
            dim_municipio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(get_uc_table_name("gold", "dim_municipio"))
            dim_periodo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(get_uc_table_name("gold", "dim_periodo"))
            fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(get_uc_table_name("gold", "fact_colegio_periodo"))
            fact_anomalia.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(get_uc_table_name("gold", "fact_anomalia"))
            logger.info("Capa Oro completada en Unity Catalog.")
            return {
                "dim_colegio": get_uc_table_name("gold", "dim_colegio"),
                "dim_municipio": get_uc_table_name("gold", "dim_municipio"),
                "dim_periodo": get_uc_table_name("gold", "dim_periodo"),
                "fact_colegio_periodo": get_uc_table_name("gold", "fact_colegio_periodo"),
                "fact_anomalia": get_uc_table_name("gold", "fact_anomalia"),
            }

        GOLD_PATH.mkdir(parents=True, exist_ok=True)
        dim_colegio.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_colegio"))
        dim_municipio.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_municipio"))
        dim_periodo.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_periodo"))
        fact.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "fact_colegio_periodo"))
        fact_anomalia.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "fact_anomalia"))

        logger.info("Capa Oro completada en %s", GOLD_PATH)
        return {
            "dim_colegio": str(GOLD_PATH / "dim_colegio"),
            "dim_municipio": str(GOLD_PATH / "dim_municipio"),
            "dim_periodo": str(GOLD_PATH / "dim_periodo"),
            "fact_colegio_periodo": str(GOLD_PATH / "fact_colegio_periodo"),
            "fact_anomalia": str(GOLD_PATH / "fact_anomalia"),
        }
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejecuta la capa Oro del pipeline Saber 11")
    parser.add_argument("--periodos", nargs="*", default=None, help="Periodos a procesar, por ejemplo 20194 20224")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    run_gold(periodos=parse_args().periodos)
