from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
import pyspark.sql.functions as f

spark = SparkSession.builder.appName('Exam').getOrCreate()

# Read the file
df_comics = spark.read.option("header", True).csv("../resources/input/csv/comics/comics.csv")
df_char_to_cm = spark.read.option("header", True).csv("../resources/input/csv/comics/charactersToComics.csv")
df_characters = spark.read.option("header", True).csv("../resources/input/csv/comics/characters.csv")

# renamed the column to avoid duplicates
df_comics = df_comics.withColumnRenamed('comicID', 'comic_ID')

# Left joins to recover the data
left_join = df_comics.join(df_char_to_cm, df_comics.comic_ID == df_char_to_cm.comicID, how='left')
second_left_join = left_join.join(df_characters, left_join.characterID == df_characters.characterID, how='left')

# Export to parquet
second_left_join.write.parquet("../output/parquet/e1/e1.parquet")