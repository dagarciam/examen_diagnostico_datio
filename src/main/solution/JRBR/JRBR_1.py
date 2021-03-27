from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import col, split
from pyspark.sql.functions import collect_set


sc=SparkContext.getOrCreate()
spark = HiveContext(sc)

\examen_diagnostico_datio\src\main\resources\input\csv\comics

def loadFile(file,columnOne,columnTwo):
	rdd = sc.textFile("/examen_diagnostico_datio/src/main/resources/input/csv/comics/" + str(file))
	rdd = rdd.map(lambda row: row.split(","))
	rdd = rdd.map(lambda p: Row(columnOne=p[0],columnTwo=p[1]))
	df = spark.createDataFrame(rdd)
	df=df.select(
	df.columnOne.alias(columnOne),
	df.columnTwo.alias(columnTwo))
	return df
	
def aJoinB(df1,df2):
	cond_join = [ df1.characterID == df2.characterID]
	df3=df1.join(df2, cond_join , "left").select(
	df1["*"],
	df2.comicID
	)
	df4 = df3.groupBy('comicID').agg(collect_set('characterID').alias('characterID'))
	df4.write.format("parquet").saveAsTable(TABLE)
	


def main():
	df_charactersToComics=loadFile("charactersToComics.csv","comicID","characterID")
	df_characters=loadFile("characters.csv","characterID","name")	
	aJoinB(rdd_characters,rdd_charactersToComics)
	
if __name__== "__main__":
    main()

