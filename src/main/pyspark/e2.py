from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
import pyspark.sql.functions as f

spark = SparkSession.builder.appName('Exam_e2').getOrCreate()

# Read the file
df = spark.read.option("header", True).csv("../resources/input/csv/fifa/players_20.csv")

# Punto 1 top_20_under_23
df = df.withColumn('top_20_under_23', f.when((f.col("age") < 23) , True).otherwise(False))

# Punto 2 top_15_by_nationality
df = df.withColumn('top_15_by_nationality', f.when((f.col("overall") > 90), True).otherwise(False))

# Punto 3 IMC
df = df.withColumn('IMC', f.col('height_cm')/f.col('weight_kg'))

# Punto 4 culbs_top
df = df.withColumn('culbs_top', f.when((f.col("overall") > 90), True).otherwise(False))

# Export to parquet
df.write.parquet("../output/parquet/e2/e2.parquet")