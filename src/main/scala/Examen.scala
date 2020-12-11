
import constants.Constants
import exercises.Exercises
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Examen {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Examen").master("local[*]").getOrCreate()

    val obj = new Exercises(spark)
    obj.run


  }
}
