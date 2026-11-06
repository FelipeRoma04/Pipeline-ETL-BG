"""Comprueba que el entorno para PySpark + Delta Lake está listo."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def ensure(condition: bool, message: str) -> None:
    if condition:
        print(f"[OK] {message}")
    else:
        print(f"[FAIL] {message}")


def cmd_exists(name: str) -> bool:
    return os.system(f"where {name} >nul 2>&1") == 0


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    data_tmp = repo_root / "data" / "tmp"
    data_tmp.mkdir(parents=True, exist_ok=True)

    print("=== Comprobacion del entorno ===")
    print(f"Python: {sys.version.split()[0]}")
    ensure(sys.version_info >= (3, 11), "Python >= 3.11")

    java_ok = cmd_exists("java")
    ensure(java_ok, "Java disponible en PATH")
    if os.getenv("JAVA_HOME"):
        ensure(Path(os.environ["JAVA_HOME"]).exists(), "JAVA_HOME apunta a un directorio válido")
    else:
        print("[WARN] JAVA_HOME no está definido. En Windows normalmente debe apuntar a un JDK (8, 11 o 17).")

    hadoop_home = os.getenv("HADOOP_HOME")
    if hadoop_home:
        ensure(Path(hadoop_home).exists(), "HADOOP_HOME apunta a un directorio válido")
    else:
        print("[WARN] HADOOP_HOME no está definido; en Windows suele no ser necesario para Spark local, pero puede ser requerido por conectores o rutas especiales.")

    try:
        import pyspark
        ensure(True, f"pyspark importado: {pyspark.__version__}")
    except Exception as exc:  # pragma: no cover
        ensure(False, f"No se pudo importar pyspark: {exc}")
        return 1

    try:
        from delta import configure_spark_with_delta_pip
        ensure(True, "delta importado correctamente")
    except Exception as exc:  # pragma: no cover
        ensure(False, f"No se pudo importar delta: {exc}")
        return 1

    try:
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[*]")
            .appName("check_env_delta")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        df = spark.createDataFrame([(1, "ok"), (2, "delta")], ["id", "value"])
        delta_path = str(data_tmp / "delta_test")
        df.write.format("delta").mode("overwrite").save(delta_path)
        result = spark.read.format("delta").load(delta_path).count()

        ensure(result == 2, f"Se escribió y leyó una tabla Delta de prueba en {delta_path} (filas={result})")
        spark.stop()
    except Exception as exc:  # pragma: no cover
        print(f"[FAIL] Error arrancando SparkSession o escribiendo Delta: {exc}")
        return 1

    print("\nResumen: el entorno parece listo para PySpark + Delta Lake.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
