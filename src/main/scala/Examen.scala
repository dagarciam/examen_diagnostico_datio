import constants.Constants
import exercises.Exercises
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Examen {
  def main(args: Array[String]): Unit = {

    println(Constants.MESSAGE)

    // Set log level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark session
    val spark = SparkSession
      .builder
      .appName("Examen")
      .master("local[4]")
      .getOrCreate()

    // Instantiate examen
    import spark.implicits._
    val examen = new Exercises(spark)

    // Run exercise 1
    examen.ejercicio1

    // Run exercise 2
    examen.ejercicio2

    // Stop Spark
    spark.stop()
  }
}
