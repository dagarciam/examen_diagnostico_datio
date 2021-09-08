package exercises

import org.apache.spark.sql.{DataFrame, SparkSession}

class Exercises(spark: SparkSession) {

  def exercise1():Unit= {

    val ambientacion = new Ambientacion(spark)
    val characterDF = ambientacion.createCharacter()
    val comicDF = ambientacion.createComic()
    val intermediateDF = ambientacion.createIntermediate()
    val unionDataframe = new UnionDataframe(spark)
    val finalReport = unionDataframe.joinReport(comicDF, characterDF, intermediateDF)
    finalReport.show(100)
  }
  def exercise2():Unit= {
    val ambientacion = new Ambientacion(spark)
    val player = ambientacion.createPlayers()
    val playersProces = new PlayersProces(spark)
    val process = playersProces.process23(player)
    process.show()
    val nationality = playersProces.processNationality(process)
    nationality.show()

    val avgDF = playersProces.processAVG(nationality)
    avgDF.show()

    avgDF.coalesce(1).write.partitionBy("nationality").mode("overwrite")
      .format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", ",")
      .save("src/main/output/parquet/e2")
  }


}
