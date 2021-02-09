val ds=sqlContext.createDataFrame(dataset)
import sqlContext.implicits._
import org.apache.spark.sql.functions.col

val dataset =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/comics.csv")

val dataset3 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/charactersToComics.csv")

val dataset2 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/characters")

val fft2 = dataset.join(dataset3,"comicID").join(dataset2,"characterID")

val sqlResult = fft2.select("comicID","title","issueNumber","description","name")

sqlResult.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Parquet")
