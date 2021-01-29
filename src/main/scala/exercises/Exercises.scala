package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


class Exercises(spark: SparkSession) {

//Ex1
  import spark.implicits._

  val dfO = spark.read.csv("src/main/resources/input/csv/comics/characters.csv")
  val dfT = spark.read.csv("src/main/resources/input/csv/comics/charactersToComics.csv")
  val dfTi = spark.read.csv("src/main/resources/input/csv/comics/comics.csv")

  val cruC = dfTi.as("pivote").join(dfT.as("adicional"),
    col("pivote.comicID") === col("adicional.comicID"),
    "left").select("pivote.*","adicional.characterID")

  val cruD = cruC.as("pivote").join(dfO.as("adicional"),
    col("pivote.characterID") === col("adicional.characterID"),
    "left").select("pivote.*","adicional.name")

  var dfCCnct = cruD.groupBy("comicID", "title").
    agg(concat_ws(",", collect_list("name")).as("name"))


  dfCCnct.write.parquet("src/main/output/parquet/comicNames.parquet")


  //Ex2
  val dfp2 = spark.read.csv("src/main/resources/input/csv/fifa/players_20.csv")


  //uno
  val m20an = dfp2.filter("age < 23").orderBy($"potential".desc).limit(20)
  m20an.write.parquet("src/main/output/parquet/plyrTop.parquet")




  //dos
  val dfSumAvg = dfp2.groupBy("club").
    agg(
      sum($"overall").as("Sum"),
      avg($"overall").as("Average"),
      collect_list($"short_name").as("Names"))

  val ansTo = dfSumAvg.select(col("Average"), col("club")).orderBy($"Average".desc).limit(20)
  ansTo.write.parquet("src/main/output/parquet/clubTop.parquet")





  //tres
  val porter = dfp2.filter("team_position = 'GK'")
  val dfSumAvgPorter = porter.groupBy("club","team_position","short_name").
    agg(
      sum($"overall").as("Sum"),
      avg($"overall").as("Averagepl"))

  import org.apache.spark.sql.expressions.Window
  val rang = dfSumAvgPorter.withColumn("rank", rank().over(Window.partitionBy("club").orderBy($"Averagepl".desc))).filter($"rank" <= 3)


  val delant = dfp2.filter("team_position = 'CAM'")
  val dfSumAvgDelant = delant.groupBy("club","team_position","short_name").
    agg(
      sum($"overall").as("Suma"),
      avg($"overall").as("Averagedl"))

  val rangDel = dfSumAvgDelant.withColumn("rank", rank().over(Window.partitionBy("club").orderBy($"Averagedl".desc))).filter($"rank" <= 3)

  val unGKCAM = rang.union(rangDel).distinct()


  val cruTrAns = ansTo.as("pivote").join(unGKCAM.as("adicional"),
    col("pivote.club") === col("adicional.club"),
    "left").select("pivote.*","adicional.Averagepl","adicional.short_name","adicional.team_position")


  cruTrAns.write.parquet("src/main/output/parquet/porteroDelantero.parquet")





  //cuatro
  val dfSumAvgNat = dfp2.groupBy("nationality","short_name").
    agg(
      sum($"overall").as("Sumanat"),
      avg($"overall").as("AverageNat"))


  val rangNat = dfSumAvgNat.withColumn("rank", rank().over(Window.partitionBy("nationality").orderBy($"AverageNat".desc))).filter($"rank" <= 5)

  rangNat.write.parquet("src/main/output/parquet/rankNacionalidad.parquet")


  //Ex3

  

}
