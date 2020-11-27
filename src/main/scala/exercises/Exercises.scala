package exercises

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Exercises(spark: SparkSession) {

  def ejercicio1: Unit = {
    //EJERCICIO 1
    //Al Dataframe que contiene los nombres de comics queremos agregar una columna que contenga los personajes a forma de array

    // Read csvs
    var df_comics = spark.read.option("header","true").csv("src/main/resources/input/csv/comics/comics.csv")
    val df_characters_to_comics = spark.read.option("header","true").csv("src/main/resources/input/csv/comics/charactersToComics.csv")
    val df_characters = spark.read.option("header","true").csv("src/main/resources/input/csv/comics/characters.csv")

    // Left join of characters_to_comics with characters to know which characters appear in each comicID
    var comics_to_characters = df_characters_to_comics.join(df_characters, Seq("characterID"), "leftouter")

    // Aggregate names to obtain array of names
    comics_to_characters = comics_to_characters
      .groupBy("comicID")
      .agg(collect_list("name").as("characters"))

    // Left join with df_comics to add the array column
    df_comics = df_comics.join(comics_to_characters, Seq("comicID"), "leftouter")

    // Subset interesting columns
    val final_comics = df_comics
      .select(
        df_comics("title"),
        df_comics("issueNumber"),
        df_comics("characters"),
      )

    // Show final schema
    final_comics.printSchema()

    // Show head
    final_comics.collect().take(10).foreach(println)

    // Write output
    final_comics.write.parquet("Ejercicio_1.parquet")
  }

  
  
  

                   
}
