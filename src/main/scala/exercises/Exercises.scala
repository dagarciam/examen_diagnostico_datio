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

    // Write output
    final_comics.write.parquet("src/main/output/ex_1.parquet")
  }

  def ejercicio2: Unit = {

    //EJERCICIO 2
    //Dado el archivo players_20.csvn nuestro coach Ramón necesita saber

    //Cuales son los 20 jugadores de menos de 23 años que tienen más potencial
    //Cuales son los equipos top 20 con el promedio (overall) más alto
    //De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con mejor (overall)
    //Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a los 5 mejores de cada país.

    // Load csv
    var df_players = spark.read.option("header","true").csv("src/main/resources/input/csv/fifa/players_20.csv")

    // Top 20 players with less than 23 years
    val df_top_20 = df_players
      .filter(df_players("age") < 23)
      .sort(df_players("potential").desc)
      .select(
        df_players("long_name"),
        df_players("age"),
        df_players("potential")
      )
      .limit(20)

    df_top_20.show(20)
    df_top_20.write.parquet("src/main/output/ex_2_a.parquet")


    // Top 20 teams with best overall
    var df_top_20_teams = df_players
      .groupBy(df_players("club"))
      .agg(avg(df_players("overall")).as("avg_overall"))

    df_top_20_teams = df_top_20_teams
      .sort(df_top_20_teams("avg_overall").desc)
      .select(
        df_players("club"),
        df_players("avg_overall")
      )
      .limit(20)

    df_top_20_teams.show(20)
    df_top_20.write.parquet("src/main/output/ex_2_b.parquet")



  }
  
  

                   
}
