from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, split
from pyspark.sql.functions import col, when, max ,avg
from pyspark.sql.types import *

sc=SparkContext.getOrCreate()
spark = HiveContext(sc)
##Carga el archivo y lo retorna como dataframe
def loadFile():
	rdd_pokemon = sc.textFile("/examen_diagnostico_datio/src/main/resources/input/csv/comics/PokemonDatat.txt")
	header = rdd_pokemon.first()
	fields = [StructField(field_name, StringType(), True) for field_name in header.split('|')]
	schema = StructType(fields)
	file_txt = rdd_pokemon.filter(lambda line: line != header) 
	file_txtSplit = file_txt.map(lambda k: k.split("|"))
	df_file = spark.createDataFrame(file_txtSplit, schema)
	return df_file

##calculo de promedio para columna sp_attack, sp_defense, speed
def avgFireWater(dfIn,columAvg):
	name="avg_" +str(columAvg) + "_"
	dfFilter=dfIn.filter(dfIn.type1.isin('fire','water')).select(
	dfIn.generation,
	dfIn.type1,
	col(columAvg)
	)
	generations = sorted(dfFilter.select("type1")
	.distinct()
	.map(lambda row: row[0])
	.collect())
	
	cols = [when(col("type1") == m, col(columAvg)).otherwise(None).alias(m) 
    for m in  generations]
	avgs = [avg(col(m)).alias(name + m) for m in generations]
	
	df_result = (dfFilter
    .select(col("generation"),col("type1"), *cols)
    .groupBy("generation","type1")
    .agg(*avgs)
    .na.fill(0))
	df_result.show()
	return df_result

##sabana final
def tabResult(dfIn,dfIn2,columUno,columnDos):
	cond_join = [ dfIn.generation == dfIn2.generation]
	df3=dfIn.join(dfIn2, cond_join , "left").select(dfIn["*"],col(columUno),col(columnDos))
	df3.write.format("parquet").saveAsTable(TABLE)
	return df3
	
def main():
	df=loadFile()	
	avg_sp_attack=avgFireWater(df,"sp_attack")
	avg_sp_defense=avgFireWater(df,"sp_defense")
	avg_speed=avgFireWater(df,"speed")
	df_join1=tabResult(avg_sp_attack,avg_sp_defense,"avg_sp_defense_fire","avg_sp_defense_water")
	df_join2=tabResult(df_join1,avg_speed,"avg_speed_fire","avg_speed_water")
	df_join2.show()

if __name__== "__main__":
	main()

