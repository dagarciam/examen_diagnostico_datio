import org.apache.spark.sql.SparkSession
object Examen {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .getOrCreate()

    val exer = new exercises.Exercises(spark)
    exer.comicsExecCmd("src/main/resources/output/parquet/")
    exer.playersExecCmd("src/main/resources/input/csv/fifa/players_20.csv", "src/main/resources/output/parquet/")
    exer.pokemonExecCmd("src/main/resources/input/csv/pokemon/PokemonData.csv", "src/main/resources/output/parquet/")

  }
}
