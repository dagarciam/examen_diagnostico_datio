import exercises.Exercises
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Examen {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("examen")
      .master("local[*]")
      .getOrCreate()

    val examen = new Exercises(spark)

    examen.exercise1()
    examen.exercise2()
    examen.exercise3()
  }
}
