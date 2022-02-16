# Author: Thamme Gowda
# Created on: Feb 2022

from pyspark.sql import SparkSession
import pyspark
from os import environ

# PLEASE SET THESE VARS
cpus = int(environ.get('SPARK_CPUS', '8'))
memory = environ.get('SPARK_MEM', '80G')
tmp_dir = environ.get('SPARK_TMPDIR', environ.get("TMPDIR", "/tmp"))


spark = SparkSession.builder \
    .master(f"local[{cpus}]") \
    .appName("RTG Many-English MT on PySpark") \
    .config(f"spark.driver.memory", memory) \
    .config(f"spark.local.dir", tmp_dir) \
    .getOrCreate()

#spark
