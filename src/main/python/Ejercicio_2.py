from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


if __name__== "__main__":

	spark = SparkSession.builder.appName("Ej_2").getOrCreate()

	file = "file:/home/hadoop/Datio/src/main/resources/input/csv/fifa/"

	file_o = "file:/home/hadoop/Datio/src/main/output/parquet/"

	df_players_20 = spark.read.format("csv").option("header", "true").load(file + "players_20.csv")

	## 1.- Cuales son los 20 jugadores de menos de 23 años que tienen más potencial

	e1_df_players_20 = df_players_20.filter(df_players_20["age"] < 23).orderBy("potential","value_eur", ascending=False).limit(20)

	e1_df_players_20.write.parquet(file_o + "players_20_2_1.parquet")

	## 2.- Cuales son los equipos top 20 con el promedio (overall) más alto

	e2_avgOverall = df_players_20.groupBy("club").agg(F.mean('overall').alias("Avg_overall")).orderBy("Avg_overall", ascending=False).limit(20)

	df_players_20.write.parquet(file_o + "players_20_2_2.parquet")

	## 3- De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con mejor (overall)

	top_20 = e2_avgOverall.take(20)
	top20 = [club["club"] for club in top_20]
	portero = ["GK"]
	delantero = ["LF","LS","CF","SS","RS","ST","RF"]

	e3_delantero = df_players_20.filter(df_players_20["club"].isin(top20) ).where(F.col('player_positions').rlike('|'.join(delantero))).orderBy("overall", ascending=False).limit(3)

	e3_delantero.write.parquet(file_o + "players_20_2_3(delantero).parquet")

	e3_portero = df_players_20.filter(df_players_20["club"].isin(top20) ).where(F.col('player_positions').rlike('|'.join(portero))).orderBy("overall", ascending=False).limit(3)

	e3_portero.write.parquet(file_o + "players_20_2_3(portero).parquet")

	## 4- Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a los 5 mejores de cada país.

	e4_rank_players = df_players_20.withColumn("row_num", F.row_number().over(Window.partitionBy("nationality").orderBy(F.col("overall").desc()))).filter(F.col("row_num") <= 5).drop("row_num")

	e4_rank_players.write.parquet(file_o + "players_20_2_4.parquet")
