package exercises

import constants.Constants
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

class Exercises(spark: SparkSession) {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def exercise1(): Unit = {

    val dfCharacters = readCsv(Constants.DIRECTORY_COMICS, Constants.COMICS_1)
    val dfCharactersToComics = readCsv(Constants.DIRECTORY_COMICS, Constants.COMICS_2)
    val dfComics = readCsv(Constants.DIRECTORY_COMICS, Constants.COMICS_3)

    var df = dfCharactersToComics.join(dfCharacters,dfCharacters("characterID") === dfCharactersToComics("characterID"),"inner")
    df = df.groupBy("comicID").agg(collect_list("name").as("personajes"))
    df = dfComics.as("dfComics").join(df.as("df"),dfComics("comicID") === df("comicID"),"inner").select("dfComics.*","df.personajes")

    saveParquet(df,Constants.OUTPUT_PATH_EJERCICIO_1)
  }

  def exercise2(): Unit = {

    val dfPlayers_20 = readCsv(Constants.DIRECTORY_FIFA, Constants.FIFA_1)

    val df1 = dfPlayers_20.filter(col("age") < 23).orderBy(col("potential").desc).limit(20)
    saveParquet(df1,Constants.OUTPUT_PATH_EJERCICIO_2A)

    val df2 = dfPlayers_20.groupBy(col("club")).agg(avg(col("overall")).as("avg_overall")).orderBy(col("avg_overall").desc).limit(20)
    saveParquet(df2,Constants.OUTPUT_PATH_EJERCICIO_2B)

    val df3a = df2.as("top20_club_avg_overall").join(dfPlayers_20.as("players_20"),df2("club") === dfPlayers_20("club")).filter(col("team_position") === "GK").orderBy(col("overall").desc).select("players_20.*").limit(3)
    val df3d = df2.as("top20_club_avg_overall").join(dfPlayers_20.as("players_20"),df2("club") === dfPlayers_20("club")).filter(col("team_position").isin(List("LS","ST","RS","LW","LF","CF","RF","RW"):_*)).orderBy(col("overall").desc).select("players_20.*").limit(3)
    saveParquet(df3a.union(df3d),Constants.OUTPUT_PATH_EJERCICIO_2C)

    val table_temp = "dfPlayers_20"
    dfPlayers_20.createOrReplaceTempView(table_temp)
    val df4 = spark.sql("""
      SELECT *
      FROM (
          select *, row_number() over
          (partition by nationality order by overall desc ) as seq
          from """+table_temp+""" as c ) c
      where seq < 6
      order by nationality """)
    saveParquet(df4,Constants.OUTPUT_PATH_EJERCICIO_2D)
  }

  def exercise3(): Unit = {

    val dfPokemonData = readCsv(Constants.DIRECTORY_POKEMON, Constants.POKEMON_1)

    val df1 = dfPokemonData.filter(col("type1") === "water").groupBy(col("generation")).agg(avg(col("sp_attack")).as("avg_sp_attack_water"), avg(col("sp_defense")).as("avg_sp_defense_water"), avg(col("speed")).as("avg_speed_water"))
    val df2 = dfPokemonData.filter(col("type1") === "fire").groupBy(col("generation")).agg(avg(col("sp_attack")).as("avg_sp_attack_fire"), avg(col("sp_defense")).as("avg_sp_defense_fire"), avg(col("speed")).as("avg_speed_fire"))
    val df = df1.as("df1").join(df2.as("df2"), df1("generation") === df2("generation"), "inner").select("df1.generation", "df1.avg_sp_attack_water", "df2.avg_sp_attack_fire", "df1.avg_sp_defense_water", "df2.avg_sp_defense_fire", "df1.avg_speed_water", "df2.avg_speed_fire").orderBy(col("generation").asc)

    saveParquet(df,Constants.OUTPUT_PATH_EJERCICIO_3)
  }

  def saveParquet(df : DataFrame, outputDirectory : String): Unit = {
    df.write.format(Constants.OUTPUT_FORMAT).mode(SaveMode.Overwrite).save(Constants.OUTPUT_PATH+outputDirectory)
  }

  def readCsv(directory: String, file_name: String): DataFrame = {
    spark.read.format(Constants.INPUT_FORMAT)
      .option(Constants.OPTION_HEADER,Constants.INPUT_HEADER)
      .option(Constants.OPTION_DELIMITER,Constants.INPUT_DELIMITER)
      .option(Constants.OPTION_QUOTE,Constants.INPUT_QUOTE)
      .load(Constants.INPUT_PATH + directory + file_name)
  }

}
