package exercises
import constants.Constants
import exercises.Util.readCsvFromPath
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}


class Exercises(spark: SparkSession) {
  def ejercicio1(): Unit ={
    val characters = readCsvFromPath(spark,"comics/characters.csv")
    val charactersToComics = readCsvFromPath(spark,"comics/charactersToComics.csv")
    val comics = readCsvFromPath(spark,"comics/comics.csv")

    characters.createOrReplaceTempView("characters")
    charactersToComics.createOrReplaceTempView("charactersToComics")
    comics.createOrReplaceTempView("comics")

    var df_1 : DataFrame = null
    df_1 = spark.sql(
      """
        select
         t2.*,
         t3.name
        from
         charactersToComics as t1
        inner join comics as t2 on t2.comicID = t1.comicID
        inner join characters as t3 on t3.characterID = t1.characterID
        """)
    df_1 = df_1.groupBy("comicID", "title", "issueNumber", "description")
      .agg(collect_list("name") as "name")
    df_1.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio1/1")
  }

  def ejercicio2(): Unit ={
    val players_20 = readCsvFromPath(spark,"fifa/players_20.csv")
    players_20.createOrReplaceTempView("players")
    var df_1 : DataFrame = null
    var df_2 : DataFrame = null
    var df_3 : DataFrame = null
    var df_4 : DataFrame = null

    df_1 = spark.sql(
      """
        select
         long_name
        from
         players
        where
          age<23
        order by potential desc
        limit 20
        """)

    df_2 = spark.sql(
      """
        select
         club,avg(overall) as overall_promedio
        from
         players
        group by club
        order by overall_promedio desc
        limit 20
        """)
    df_2.createOrReplaceTempView("20_mejores_clubes")

    df_3 = spark.sql(
      """
         select
          t2.long_name,
          t2.overall,
          t2.player_positions

         from 20_mejores_clubes as t1
         inner join players t2 on t2.club=t1.club
         where
          t2.player_positions like '%GK%'
          or
          t2.player_positions like '%SS%'
          or
          t2.player_positions like '%RW%'
          or
          t2.player_positions like '%CF%'
        """)
    val df_3_1:DataFrame = df_3.filter(col("player_positions").like("%SS%") or
      col("player_positions").like("%RW%") or
      col("player_positions").like("%CF%")
    ).orderBy(desc("overall")).limit(3)
    val df_3_2:DataFrame = df_3.filter(col("player_positions").like("%GK%")).orderBy(desc("overall")).limit(3)
    df_3 = df_3_1.union(df_3_2)

    df_4 = spark.sql(
      """
        select
          long_name,
          nationality,
          overall
        from
          players
        """)
    val window = Window.partitionBy("nationality").orderBy(desc("overall"))
    df_4 = df_4.withColumn("row",row_number.over(window))
      .where(col("row") <= 5).drop("row")

    df_1.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/1")
    df_2.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/2")
    df_3.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/3")
    df_4.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/4")

  }

  def ejercicio3(): Unit ={
    var pokemon = readCsvFromPath(spark,"pokemon/PokemonData.csv")
    pokemon = pokemon.select("generation","type1","sp_attack","sp_defense","speed")
                      .filter(col("type1")==="fire" or col("type1")==="water")
    pokemon.createOrReplaceTempView("pokemon")
    val df = spark.sql(
      """
        select
          generation,
          type1,
          cast(sp_attack as float) sp_attack,
          cast(sp_defense as float) sp_defense,
          cast(speed as float) as speed
        from
          pokemon
        """)

    var df_fire = df.filter(col("type1")==="fire")
    var df_water = df.filter(col("type1")==="water")
    df_fire.createOrReplaceTempView("fire")
    df_water.createOrReplaceTempView("water")
    df_fire = spark.sql(
      """
        select
          generation,
          null as avg_sp_attack_water,
          avg(sp_attack) as avg_sp_attack_fire,
          null as avg_sp_defense_water,
          avg(sp_defense) as avg_sp_defense_fire,
          null as avg_speed_water,
          avg(speed) as avg_speed_fire
        from
          fire as f
        group by (generation)
        """)
    df_water = spark.sql(
      """
        select
          generation,
          avg(sp_attack) as avg_sp_attack_water,
          null as avg_sp_attack_fire,
          avg(sp_defense) as avg_sp_defense_water,
          null as avg_sp_defense_fire,
          avg(speed) as avg_speed_water,
          null as avg_speed_fire
          from
          water as w
          group by (generation)
      """)
    df_fire.createOrReplaceTempView("fire")
    df_water.createOrReplaceTempView("water")
    val df_fire_and_water = spark.sql(
      """
        select
          f.generation,
          w.avg_sp_attack_water,
          f.avg_sp_attack_fire,
          w.avg_sp_defense_water,
          f.avg_sp_defense_fire,
          w.avg_speed_water,
          f.avg_speed_fire
        from
          fire as f
        inner join water as w on w.generation=f.generation
        order by generation asc
      """)
    df_fire_and_water.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio3/1")

  }
}

object Util {
  def getSparkSession(): SparkSession ={
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("Prueba_Indra")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.cores.max","3")
    sparkConf.set("spark.executor.memory","4G")
    sparkConf.set("spark.executor.cores","1")
    sparkConf.set("spark.executor.","1")
    sparkConf.set("spark.dynamicAllocation.enabled","false")
    sparkConf.setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    spark
  }

  def readCsvFromPath(spark:SparkSession,path:String): DataFrame ={
    spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", true)
      .load(Constants.INPUT_PATH+path)
  }
}