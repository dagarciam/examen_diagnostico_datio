package exercises
import constants.Constants
import exercises.Util.readCsvFromPath
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, collect_list, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}


class Exercises(spark: SparkSession) {
  def ejercicio1(): Unit ={
    val characters = readCsvFromPath(spark,"comics/characters.csv")
    val charactersToComics = readCsvFromPath(spark,"comics/charactersToComics.csv")
    val comics = readCsvFromPath(spark,"comics/comics.csv")
    var df_1 : DataFrame = null

    df_1 = charactersToComics.as("t1")
      .join(
        comics.as("t2"),
        charactersToComics("comicID")===col("t2.comicID"),
        "inner")
      .join(
        characters.as("t3"),
        charactersToComics("characterID")===col("t3.characterID"),
        "inner")
      .select(col("t2.*"),col("t3.name"))
      .groupBy("comicID", "title", "issueNumber", "description")
      .agg(collect_list("name") as "name")

    df_1.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio1/1")
  }

  def ejercicio2(): Unit ={
    val players_20 = readCsvFromPath(spark,"fifa/players_20.csv")
    val df_1 = players_20
      .select(col("long_name"))
      .where(col("age")<23)
      .orderBy(desc("potential"))
      .limit(20)

    val df_2 = players_20
      .groupBy(col("club"))
      .agg(avg(col("overall")).as( "overall_promedio"))
      .orderBy(desc("overall_promedio"))
      .select(col("club"),col("overall_promedio"))
      .limit(20)

    var df_3 = df_2.as("t1")
      .join(
        players_20.as("t2"),
        df_2("club")===col("t2.club"),
        "inner")
      .select("t2.long_name",
              "t2.overall",
              "t2.player_positions")
      .filter(
        col("t2.player_positions").like("%GK%") or
        col("t2.player_positions").like("%SS%") or
        col("t2.player_positions").like("%RW%") or
        col("t2.player_positions").like("%CF%")
      )

    val df_3_1 = df_3
      .filter(
        col("player_positions").like("%SS%") or
        col("player_positions").like("%RW%") or
        col("player_positions").like("%CF%"))
      .orderBy(desc("overall"))
      .limit(3)
    val df_3_2 = df_3
      .filter(
        col("player_positions").like("%GK%"))
      .orderBy(desc("overall")).limit(3)
    df_3 = df_3_1.union(df_3_2)


    val window = Window.partitionBy("nationality").orderBy(desc("overall"))
    val df_4 = players_20
      .select("long_name","nationality","overall")
      .withColumn("row",row_number.over(window))
      .where(col("row") <= 5).drop("row")

    df_1.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/1")
    df_2.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/2")
    df_3.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/3")
    df_4.write.mode("overwrite").parquet(Constants.OUTPUT_PATH+ "ejercicio2/4")

  }

  def ejercicio3(): Unit ={
    val pokemon = readCsvFromPath(spark,"pokemon/PokemonData.csv")
    val df_pokemons = pokemon
      .select(
        col("generation"),
        col("type1"),
        col("sp_attack").cast("float"),
        col("sp_defense").cast("float"),
        col("speed").cast("float"))
      .filter(
        col("type1")==="fire" or
        col("type1")==="water")

    var df_fire = df_pokemons.filter(col("type1")==="fire")
    var df_water = df_pokemons.filter(col("type1")==="water")

    df_fire = df_fire
      .groupBy("generation")
      .agg(
        avg("sp_attack").as("avg_sp_attack_fire"),
        avg("sp_defense").as("avg_sp_defense_fire"),
        avg("speed").as("avg_speed_fire")
      )
      .select(
        col("generation"),
        col("avg_sp_attack_fire"),
        col("avg_sp_defense_fire"),
        col("avg_speed_fire")
      )

    df_water = df_water
      .groupBy("generation")
      .agg(
        avg("sp_attack").as("avg_sp_attack_water"),
        avg("sp_defense").as("avg_sp_defense_water"),
        avg("speed").as("avg_speed_water")
      )
      .select(
        col("generation"),
        col("avg_sp_attack_water"),
        col("avg_sp_defense_water"),
        col("avg_speed_water")
      )

    val df_fire_and_water = df_fire.as("f")
      .join(
        df_water.as("w"),
        col("f.generation")===col("w.generation"),
        "inner")
      .select(
        "f.generation",
        "w.avg_sp_attack_water",
        "f.avg_sp_attack_fire",
        "w.avg_sp_defense_water",
        "f.avg_sp_defense_fire",
        "w.avg_speed_water",
        "f.avg_speed_fire")
      .orderBy(asc("generation"))

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