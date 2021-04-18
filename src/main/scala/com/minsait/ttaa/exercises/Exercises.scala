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
  
}
