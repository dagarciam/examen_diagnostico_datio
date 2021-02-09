
val dataset =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/comics.csv")

val dataset3 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/charactersToComics.csv")

val dataset2 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/characters")

val sqlResult = fft2.select("comicID","title","issueNumber","description","name")

sqlResult.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Parquet")
