import constants.Constants
import exercises.Exercises
import org.apache.spark.sql.SparkSession

object Examen {
  def main(args: Array[String]): Unit = {

    println(Constants.MESSAGE)

    // Spark session
    val spark = SparkSession
      .builder
      .appName("Examen")
      .master("local[4]")
      .getOrCreate()

    // Instantiate examen
    val examen = new Exercises(spark)


    // Stop Spark
    spark.stop()
  }
}
