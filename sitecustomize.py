import os

os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
path = os.environ.get("PATH", "")
winutils_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if winutils_bin not in path:
    os.environ["PATH"] = path + (";" if path else "") + winutils_bin
