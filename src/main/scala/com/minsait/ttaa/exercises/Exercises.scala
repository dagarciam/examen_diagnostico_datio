package com.minsait.ttaa.exercises

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, collect_list, max, pow, row_number, when}
import org.apache.spark.storage.StorageLevel

class Exercises(spark: SparkSession) {

  def exercise01(): Unit = {
    val charactersFile = "src/main/resources/input/csv/comics/characters.csv"
    val comicsFile = "src/main/resources/input/csv/comics/comics.csv"
    val charactersToComicsFile = "src/main/resources/input/csv/comics/charactersToComics.csv"

    val charactersCSV = new FileReader(FileStrategy(charactersFile))
    val characters = charactersCSV.read(charactersFile).getOrElse(spark.emptyDataFrame)
      .persist(StorageLevel.MEMORY_ONLY_2)
      .as("characters")

    val comicsCSV = new FileReader(FileStrategy(comicsFile))
    val comics = comicsCSV.read(comicsFile).getOrElse(spark.emptyDataFrame)
      .repartition(col("comicID"))
      .persist(StorageLevel.MEMORY_ONLY_2)
      .as("comics")

    val charactersToComicsCSV = new FileReader(FileStrategy(charactersToComicsFile))
    val charactersToComics = charactersToComicsCSV.read(charactersToComicsFile).getOrElse(spark.emptyDataFrame)
      .repartition(col("comicID"))
      .persist(StorageLevel.MEMORY_ONLY_2)
      .as("charactersToComics")

    val characterName = charactersToComics
      .join(characters, charactersToComics("characterID") === characters("characterID"), "left")
      .select("charactersToComics.comicID", "charactersToComics.characterID", "characters.name")
      .groupBy(col("comicID"))
      .agg(collect_list("name").as("character_name")).as("characterName")
    val result = comics
      .join(characterName, comics("comicID") === characterName("comicID"), "left")
      .select("comics.comicID", "comics.title", "comics.issueNumber", "comics.description", "characterName.character_name")
      .repartition(col("comicID"))
    result.write.mode(SaveMode.Overwrite).parquet("src/main/output/parquet/e1")
  }

  def exercise02(): Unit = {
    val playerFile = "src/main/resources/input/csv/fifa/players_20.csv"
    val playerCSV = new FileReader(FileStrategy(playerFile))
    val players_20 = playerCSV.read(playerFile).getOrElse(spark.emptyDataFrame)
      .persist(StorageLevel.MEMORY_ONLY_2)
      .as("players_20")
    players_20.printSchema()
    players_20.show(10, false)
    val under23 = players_20
      .where(col("age") <= 23)
      .sort(col("potential").desc)
      .limit(20)
      .select(col("*"), when(col("age") <= 23, true)
        .as("under_23"))
      .select(col("sofifa_id").as("sofifa_id_under_23"), col("under_23"))
      .alias("under_23")

    val top_20_under_23 = players_20.join(under23, players_20("sofifa_id") === under23("sofifa_id_under_23"), "left_outer")
      .select(col("*"), when(col("under_23") === true, true)
        .otherwise(false).alias("top_20_under_23"))
      .select("sofifa_id_under_23", "top_20_under_23")

    val top_nationality = players_20.select(col("nationality"), col("overall").cast("Int").as("overall"))
      .groupBy("nationality")
      .agg(max("overall").alias("overall_max"))
      .select(col("nationality").alias("top_nationality"),col("overall_max"))

    val top_15_by_nationality = players_20
      .join(top_nationality, players_20("nationality") === top_nationality("top_nationality") && players_20("overall") === top_nationality("overall_max"), "left_outer")
      .select(col("*"), when(col("overall_max").isNotNull, true).otherwise(false).as("top_15_by_nationality"))
      .select(col("sofifa_id").alias("sofifa_id_nationality"), col("top_15_by_nationality"))

    val imc = players_20.select(col("nationality"), (col("weight_kg") / pow((col("height_cm") / 100), 2)).alias("imc"))
      .groupBy("nationality")
      .agg(avg("imc").alias("avg_imc"))
      .select(col("nationality").alias("imc_nationality"),col("avg_imc"))

    val top_club = players_20
      .groupBy("club")
      .agg(avg("overall").alias("avg_overall"))
      .sort(col("avg_overall").desc)
      .limit(20)
      .select(col("club").alias("top_club"),col("avg_overall"))

    val teamPosition = List("GK", "LF", "CF", "LS", "SS", "RS", "ST", "RF")

    val w = Window.partitionBy("overall").orderBy("overall")
    val top_gk = top_club.join(players_20, top_club("top_club") === players_20("club")).filter(col("team_position") === "GK").sort(col("overall")).limit(3)

    val result = players_20
      .join(top_20_under_23,players_20("sofifa_id")===top_20_under_23("sofifa_id_under_23"))
      .join(top_15_by_nationality,players_20("sofifa_id")===top_15_by_nationality("sofifa_id_nationality"))
      .join(imc,players_20("nationality")===imc("imc_nationality"))
      .join(top_club,players_20("club")===top_club("top_club"))
    result.write.mode(SaveMode.Overwrite).parquet("src/main/output/parquet/e2")
    result.show(false)
  }
}
