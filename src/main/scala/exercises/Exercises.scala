package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType.fromDDL

class Exercises(spark: SparkSession) {
  import spark.implicits._

  def comicsExecCmd(dstPath: String): Unit = {

    val charactersPath = "src/main/resources/input/csv/comics/comics.csv"
    val charToComicsPath = "src/main/resources/input/csv/comics/charactersToComics.csv"
    val comicsPath = "src/main/resources/input/csv/comics/comics.csv"

    val ddlCharRaw = "characterID STRING, name STRING"
    val ddlChar = fromDDL(ddlCharRaw)

    val dfChar = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlChar)
      .load(charactersPath)
      .as("char")

    val ddlComicRaw = "comicID STRING, title STRING, issueNumber INT, description STRING"
    val ddlComic = fromDDL(ddlComicRaw)

    val dfComics = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlComic)
      .load(comicsPath)
      .as("comics")

    val dfValidComics = dfComics.filter($"comicId".isNotNull)

    val ddlCharToComics = fromDDL("comicID STRING, characterID STRING")
    val dfCharToComics = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlCharToComics)
      .load(charToComicsPath)
      .as("ctoc")

    val dfGrp = dfValidComics.join(dfCharToComics, Seq("comicId"), "inner")
      .join(dfChar, Seq("characterId"), "inner")
      .groupBy("comicId")
      .agg(collect_list($"name").alias("characters"))

    val dfComicsRes = dfValidComics.join(dfGrp, Seq("comicId"), "inner")

    dfComicsRes
      .write
      .format("parquet")
      .mode("overwrite")
      .save(dstPath)

  }

  def playersExecCmd(srcPath: String, dstPath: String): Unit = {

    val playersPath = srcPath

    val dfPlayers = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(playersPath)
      .as("players")

    val top = 20
    val dfClubsOverall = dfPlayers
      .groupBy("club")
      .agg(avg("overall").alias("overallAvg"))
      .sort($"overallAvg".desc)

    val denseRankExp = dense_rank.over(Window.orderBy($"overallAvg".desc))

    val dfTop20Overall = dfClubsOverall
      .withColumn("topOverall", denseRankExp)
      .filter($"topOverall" <= top)

    val colsPlayers = dfPlayers.columns.map(col(_))

    val replaceExp = regexp_replace($"player_positions", "\\s*", "")

    val dfGoalKeepers = dfPlayers
      .join(broadcast(dfTop20Overall), Seq("club"), "inner")
      .select(split(replaceExp, ",").alias("playersPosArr") +: colsPlayers:_*)
      .filter(array_contains($"playersPosArr", "GK"))

    val dfGoalStrikers = dfPlayers
      .join(broadcast(dfTop20Overall), Seq("club"), "inner")
      .select(split(replaceExp, ",").alias("playersPosArr") +: colsPlayers:_*)
      .filter(array_contains($"playersPosArr", "ST"))

    val denseGKRankExp = dense_rank.over(Window.orderBy($"overall".desc))

    val denseSTRankExp = dense_rank.over(Window.orderBy($"overall".desc))

    val topGK = 3
    val dfTopGoalGK = dfGoalKeepers
      .withColumn("topOverallGK", denseGKRankExp)
      .filter($"topOverallGK" <= topGK)

    val topST = 3
    val dfTopGoalST = dfGoalStrikers
      .withColumn("topOverallST", denseSTRankExp)
      .filter($"topOverallST" <= topST)

    dfTopGoalGK
      .write
      .format("parquet")
      .mode("overwrite")
      .save(dstPath + "_GK")

    dfTopGoalST
      .write
      .format("parquet")
      .mode("overwrite")
      .save(dstPath + "_ST")
  }

  def pokemonExecCmd(srcPath: String, dstPath: String): Unit = {

    val pokemonPath = srcPath

    val dfPokemon = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(pokemonPath)
      .as("pokemon")

    val dfPokemonWater = dfPokemon
      .filter($"type1" === "water")
      .groupBy("generation")
      .agg(
        avg("sp_attack").alias("avg_sp_attack_water"),
        avg("sp_defense").alias("avg_sp_defense_water"),
        avg("speed").alias("avg_speed_water")
      )
      .sort($"generation".asc)

    val dfPokemonFire = dfPokemon
      .filter($"type1" === "fire")
      .groupBy("generation")
      .agg(
        avg("sp_attack").alias("avg_sp_attack_fire"),
        avg("sp_defense").alias("avg_sp_defense_fire"),
        avg("speed").alias("avg_speed_fire")
      )
      .sort($"generation".asc)

    val values = List("generation", "avg_sp_attack_water", "avg_sp_attack_fire", "avg_sp_defense_water", "avg_sp_defense_fire", "avg_speed_water", "avg_speed_fire")
    val selectValues = values.map(v => {
      col(v)
    })

    val dfPokemonRes = dfPokemonWater
      .join(dfPokemonFire, Seq("generation"), "inner")
      .select(selectValues:_*)

    dfPokemonRes
      .write
      .format("parquet")
      .mode("overwrite")
      .save(dstPath)

  }

}
