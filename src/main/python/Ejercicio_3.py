from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__== "__main__":

	spark = SparkSession.builder.appName("Ej_3").getOrCreate()

	file = "file:/home/hadoop/Datio/src/main/resources/input/csv/pokemon/"

	file_o = "file:/home/hadoop/Datio/src/main/output/parquet/"

	pokemon = spark.read.format("csv").option("header", "true").load(file + "PokemonData.csv")

	## Filtrando por los tipos de pokemon(type1) fire y water es necesario calcular el promedio de cada una de las siguientes 
	## columnas: sp_attack, sp_defense y speed; de tal forma que el dataframe resultante muestre los siguientes datos: 
	## generation, avg_sp_attack_water, avg_sp_attack_fire, avg_sp_defense_water, avg_sp_defense_fire, avg_speed_water, 
	## avg_speed_fire

	e3_pokemon_f = pokemon.filter(F.col("type1") == "fire" ).groupBy("generation").agg(F.mean('sp_attack').alias("avg_sp_attack_fire"),F.mean('sp_defense').alias("avg_sp_defense_fire"),F.mean('speed').alias("avg_speed_fire"))

	e3_pokemon_w = pokemon.filter(F.col("type1") == "water" ).groupBy("generation").agg(F.mean('sp_attack').alias("avg_sp_attack_water"),F.mean('sp_defense').alias("avg_sp_defense_water"),F.mean('speed').alias("avg_speed_water"))

	e3_pokemon = e3_pokemon_f.join(e3_pokemon_w, on=["generation"]).orderBy("generation", ascending=True)

	e3_pokemon.write.parquet(file_o + "Pokemon_1_1.parquet")
