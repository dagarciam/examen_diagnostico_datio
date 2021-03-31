from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct, avg, stddev 
import csv

spark = SparkSession.builder.appName('Exam_e3').getOrCreate()

# Read the file
df = spark.read.option("header", True).csv("../resources/input/csv/pokemon/PokemonData.csv")

# Filter by fire and water
df_filtered = df.filter( (f.col("type1") == 'fire') | (f.col("type1") == 'water') )

# avg by columns
df_filtered.groupBy('type1').agg( avg('generation').alias('generation'), f.avg('sp_attack').alias('sp_attack'), f.avg('sp_defense').alias('sp_defense'), f.avg('speed').alias('sp_speed')).show(5)

# Export to parquet
df.write.parquet("../output/parquet/e3/e3.parquet")