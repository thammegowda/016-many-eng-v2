# Author: Thamme Gowda
# Created on: Feb 2022

from pyspark.sql import SparkSession
import pyspark

# please edit this section; give the highest available
cpus = 60
memory = '900g'  # yeah! thanks USC CARC for largemem nodes! 1TB RAM is no joke :-D 

cpus = 50
memory = '500g'

tmp_dir = "/scratch2/tnarayan/tmp/spark"

spark = SparkSession.builder \
    .master(f"local[{cpus}]") \
    .appName("RTG Many-English MT on PySpark") \
    .config(f"spark.driver.memory", memory) \
    .config(f"spark.local.dir", tmp_dir) \
    .getOrCreate()

#spark
