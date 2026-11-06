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


def resolve_java_home() -> str | None:
    candidates = [
        os.getenv("JAVA_HOME"),
        "C:/Program Files/Java/jdk-22",
        "C:/Program Files/Java/jdk-21",
        "C:/Program Files/Java/jdk-17",
        "C:/Program Files/Java/jdk-11",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and (path / "bin").exists():
            return str(path)
    return None


def resolve_hadoop_home() -> str | None:
    candidates = [
        os.getenv("HADOOP_HOME"),
        "C:/hadoop",
        "C:/Hadoop",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and (path / "bin").exists():
            return str(path)
    return None


def ensure_path_contains(directory: str) -> None:
    if not directory:
        return
    current = os.environ.get("PATH", "")
    entries = [entry.strip() for entry in current.split(";") if entry.strip()]
    normalized = str(Path(directory).resolve())
    if normalized.lower() not in [entry.lower() for entry in entries]:
        entries.insert(0, normalized)
        os.environ["PATH"] = ";".join(entries)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    data_tmp = repo_root / "data" / "tmp"
    data_tmp.mkdir(parents=True, exist_ok=True)

    print("=== Comprobacion del entorno ===")
    print(f"Python: {sys.version.split()[0]}")
    ensure(sys.version_info >= (3, 11), "Python >= 3.11")

    java_home = resolve_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        ensure_path_contains(str(Path(java_home) / "bin"))
        ensure(Path(java_home).exists(), "JAVA_HOME apunta a un directorio válido")
    else:
        print("[WARN] JAVA_HOME no está definido. En Windows debe apuntar a un JDK instalado (por ejemplo, C:\\Program Files\\Java\\jdk-17).")
    ensure(cmd_exists("java"), "Java disponible en PATH")

    hadoop_home = resolve_hadoop_home()
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        ensure_path_contains(str(Path(hadoop_home) / "bin"))
        ensure(Path(hadoop_home).exists(), "HADOOP_HOME apunta a un directorio válido")
        winutils = Path(hadoop_home) / "bin" / "winutils.exe"
        ensure(winutils.exists(), f"winutils.exe existe en {winutils}")
    else:
        print("[WARN] HADOOP_HOME no está definido. En Windows Spark local puede requerir una ruta raíz y winutils.exe en %HADOOP_HOME%\\bin.")

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

        if not os.environ.get("JAVA_HOME"):
            os.environ["JAVA_HOME"] = resolve_java_home() or ""
        if not os.environ.get("HADOOP_HOME"):
            os.environ["HADOOP_HOME"] = resolve_hadoop_home() or ""

        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        builder = (
            SparkSession.builder.master("local[*]")
            .appName("check_env_delta")
            .config("spark.local.hostname", "localhost")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
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
