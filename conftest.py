import os
import sys

os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
path = os.environ.get("PATH", "")
winutils_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if winutils_bin not in path:
    os.environ["PATH"] = path + (";" if path else "") + winutils_bin
