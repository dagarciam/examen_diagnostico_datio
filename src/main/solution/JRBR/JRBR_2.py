from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, max ,avg,lit,split,coalesce

sc=SparkContext.getOrCreate()
spark = HiveContext(sc)

##cargar el archivo y lo retorna como dataframe
def loadFile():
	rdd_pokemon = sc.textFile("/examen_diagnostico_datio/src/main/resources/input/csv/comics/players_20.txt")
	header = rdd_pokemon.first()
	fields = [StructField(field_name, StringType(), True) for field_name in header.split('|')]
	schema = StructType(fields)
	file_txt = rdd_pokemon.filter(lambda line: line != header) 
	file_txtSplit = file_txt.map(lambda k: k.split("|"))
	df_file = spark.createDataFrame(file_txtSplit, schema)
	return df_file

##filtro de dataframe unicamente por columnas requeridas para reglas
def df_columns(dfIn):
	df3=dfIn.select(dfIn.sofifa_id,
	dfIn.short_name,
	dfIn.nationality,
	dfIn.overall,
	dfIn.potential,
	dfIn.age,
	dfIn.club
	).orderBy(dfIn['nationality'].asc())
	return df3

##funciones para calculos
def df_top_15_by_nationality (dfIn):
	window = Window.partitionBy(dfIn.nationality).orderBy(dfIn.overall.desc())	
	df1=dfIn.select('*', rank().over(window).alias('rank'))
	df2=df1.select(df1.sofifa_id,
	when(
	col('rank').isin(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),
	lit(1)
	).otherwise(0).alias("top_15_by_nationality")
	)
	return df2
	
def df_top_20_under_23 (dfIn):
	dfIn=dfIn.filter(dfIn.age <= 19)
	window = Window.partitionBy().orderBy(dfIn.potential.desc())	
	df1=dfIn.select('*', rank().over(window).alias('rank'))
	df2=df1.select(df1.sofifa_id,
	when(
	col('rank').isin(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),
	lit(1)
	).otherwise(0).alias("top_20_under_23")
	)
	return df2
	
def df_culbs_top (dfIn):
	window = Window.partitionBy(dfIn.club)	
	df1=dfIn.select('*', avg(dfIn.overall).over(window).alias('avg'))
	window = Window.partitionBy().orderBy(df1.avg.desc())
	df1=df1.select('*', rank().over(window).alias('rank'))
	df2=df1.select(df1.sofifa_id,
	when(
	col('rank').isin(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),
	lit(1)
	).otherwise(0).alias("culbs_top")
	)
	return df2
	

###sabana final
def tabResult(dfIn,dfIn2,columUno):
	cond_join = [ dfIn.sofifa_id == dfIn2.sofifa_id]
	df3=dfIn.join(dfIn2, cond_join , "left").select(dfIn["*"],coalesce(col(columUno),lit(0)).alias(columUno))
	return df3

def main():	
	df=loadFile()
	dfFilter=df_columns(df)
	dftop_15_by_nationality=df_top_15_by_nationality(dfFilter)
	dftop_20_under_23=df_top_20_under_23(dfFilter)
	dfdf_culbs_top=df_culbs_top(dfFilter)
	df_result=tabResult(dfFilter,dftop_15_by_nationality,"top_15_by_nationality")
	df_result=tabResult(df_result,dftop_20_under_23,"top_20_under_23")
	df_result=tabResult(df_result,dfdf_culbs_top,"culbs_top")
	df_result.write.format("parquet").saveAsTable(TABLE)
	
if __name__== "__main__":
	main()

	
