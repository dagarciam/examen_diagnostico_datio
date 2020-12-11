package exercises

import constants.Constants
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, desc, max, row_number, avg}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Exercises(spark: SparkSession) {

  def readCSV(PATH_INPUT: String): DataFrame = {
    spark.read.option("header", true).option("inferSchema", true).csv(PATH_INPUT)
  }

  def exercise1(PATH_COMICS: String, PATH_CHARACTER_TO_COMICS: String, PATH_CHARACTERS: String): DataFrame = {

    val comicsDF: DataFrame = readCSV(PATH_COMICS)
    val charactersDF: DataFrame = readCSV(PATH_CHARACTER_TO_COMICS)
    val characterToComicsDF: DataFrame = readCSV(PATH_CHARACTERS)

    val comicsJoinDF: DataFrame = comicsDF
      .join(characterToComicsDF, Seq(Constants.COL_COMIC_ID), Constants.INNER_JOIN)
      .join(charactersDF, Seq(Constants.COL_CHARACTER_ID), Constants.INNER_JOIN)

    comicsJoinDF
      .groupBy(Constants.COL_COMIC_ID,
        Constants.COL_TITLE,
        Constants.COL_ISSUE_NUMBER,
        Constants.COL_DESCRIPTION)
      .agg(collect_list(Constants.COL_NAME).alias(Constants.COL_CHARACTER_NAME))
  }

  def exercise2(PATH_PLAYERS: String): List[DataFrame] = {

    val playersDF: DataFrame = readCSV(PATH_PLAYERS)

    val questionOneDF: DataFrame = playersDF
      .orderBy(desc(Constants.COL_POTENTIAL))
      .filter(col(Constants.COL_AGE) < 23)
      .limit(20)

    val questionTwoDF: DataFrame = playersDF
      .groupBy(Constants.COL_CLUB)
      .agg(max(Constants.COL_OVERALL).alias(Constants.COL_MAX_OVERALL))
      .orderBy(desc(Constants.COL_MAX_OVERALL))
      .limit(20)

    val questionThreeGK: DataFrame = playersDF
      .join(questionTwoDF, Seq(Constants.COL_CLUB), Constants.INNER_JOIN)
      .filter(col(Constants.COL_PLAYER_POSITIONS).contains("GK"))
      .orderBy(desc(Constants.COL_OVERALL))
      .select(Constants.COL_SHORT_NAME, Constants.COL_PLAYER_POSITIONS)
      .limit(3)

    val questionThreeST: DataFrame = playersDF
      .join(questionTwoDF, Seq(Constants.COL_CLUB), Constants.INNER_JOIN)
      .filter(col(Constants.COL_PLAYER_POSITIONS).contains("ST"))
      .orderBy(desc(Constants.COL_OVERALL))
      .select(Constants.COL_SHORT_NAME, Constants.COL_PLAYER_POSITIONS)
      .limit(3)

    val questionThreeDF: DataFrame = questionThreeGK
      .union(questionThreeST)

    val windowSpec = Window.partitionBy(Constants.COL_NATIONALITY).orderBy(desc(Constants.COL_OVERALL))

    val questionFourDF = playersDF.withColumn(Constants.COL_RANKING, row_number.over(windowSpec))
      .filter(col(Constants.COL_RANKING) <= 5)
      .select(Constants.COL_NATIONALITY, Constants.COL_SHORT_NAME, Constants.COL_OVERALL, Constants.COL_RANKING)

    List(questionOneDF, questionTwoDF, questionThreeDF, questionFourDF)
  }

  def exercise3(PATH_POKEMON: String): DataFrame = {
    val pokemonDF: DataFrame = readCSV(PATH_POKEMON)

    val filterPokemonDF: DataFrame = pokemonDF
      .filter(col(Constants.COL_TYPE1).equalTo(Constants.VAL_FIRE) || col(Constants.COL_TYPE1).equalTo(Constants.VAL_WATER))
      .select(Constants.COL_GENERATION, Constants.COL_SP_ATTACK, Constants.COL_SP_DEFENSE, Constants.COL_SPEED, Constants.COL_TYPE1)


    filterPokemonDF
      .groupBy(Constants.COL_GENERATION)
      .pivot(Constants.COL_TYPE1)
      .agg(avg(col(Constants.COL_SP_ATTACK)),
        avg(col(Constants.COL_SP_DEFENSE)),
        avg(col(Constants.COL_SPEED)))
      .select(col(Constants.COL_GENERATION),
        col("water_avg(sp_attack)").alias(Constants.ALIAS_AVG_SP_ATTACK_WATER),
        col("fire_avg(sp_attack)").alias(Constants.ALIAS_AVG_SP_ATTACK_FIRE),
        col("water_avg(sp_defense)").alias(Constants.ALIAS_AVG_SP_DEFENSE_WATER),
        col("fire_avg(sp_defense)").alias(Constants.ALIAS_AVG_SP_DEFENSE_FIRE),
        col("water_avg(speed)").alias(Constants.ALIAS_AVG_SPEED_WATER),
        col("fire_avg(speed)").alias(Constants.ALIAS_AVG_SPEED_FIRE)
      )
  }

  def write(df: DataFrame, outputName: String): Unit = {
    df.coalesce(1).write.parquet(Constants.PATH_OUTPUT + outputName)
  }


  def run: Unit = {

    write(exercise1(Constants.PATH_COMICS, Constants.PATH_CHARACTER_TO_COMICS, Constants.PATH_CHARACTERS), "solution_1")
    write(exercise2(Constants.PATH_PLAYERS)(0), "solution_2.1")
    write(exercise2(Constants.PATH_PLAYERS)(1), "solution_2.2")
    write(exercise2(Constants.PATH_PLAYERS)(2), "solution_2.3")
    write(exercise2(Constants.PATH_PLAYERS)(3), "solution_2.4")
    write(exercise3(Constants.PATH_POKEMON), "solution_3")

  }
}

