import constants.Constants
import exercises.Exercises
import org.apache.spark.sql.SparkSession
object Examen {
  def main(args: Array[String]): Unit = {

    println(Constants.MESSAGE)

    val spark = SparkSession.builder()
      .config("spark.master", "local[2]")
      .getOrCreate()

 val excercise= new Exercises(spark)
 excercise.exercise1()
 excercise.exercise2()


  }
}