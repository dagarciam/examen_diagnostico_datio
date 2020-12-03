from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__== "__main__":

	spark = SparkSession.builder.appName("Ej_1").getOrCreate()

	file = "file:/home/hadoop/Datio/src/main/resources/input/csv/comics/"

	file_o = "file:/home/hadoop/Datio/src/main/output/parquet/"

	df_comics = spark.read.format("csv").option("header", "true").load(file + "comics.csv")

	df_charactersToComics = spark.read.format("csv").option("header", "true").load(file + "charactersToComics.csv")

	df_characters = spark.read.format("csv").option("header", "true").load(file + "characters.csv")

	## Al Dataframe que contiene los nombres de comics queremos agregar una columna que contenga los personajes a forma de array

	df_comics_ = df_comics.join(df_charactersToComics.join(df_characters, on = ["characterID"], how = "outer").groupBy('comicID').agg(F.collect_list("name").alias("names")), on = ["comicID"])

	df_comics_.write.parquet(file_o + "comics_1_1.parquet")