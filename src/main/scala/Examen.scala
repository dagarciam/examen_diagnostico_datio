import exercises.Exercises
import exercises.Util.getSparkSession

object Examen {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val Exercises = new Exercises(spark)
    Exercises.ejercicio1()
    Exercises.ejercicio2()
    Exercises.ejercicio3()
  }
}
