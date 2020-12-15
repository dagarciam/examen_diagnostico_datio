package constants

trait Constants {
  val MESSAGE: String = "Hola mundo"
  val INPUT_PATH: String = "./src/main/resources/input/csv/"
  val OUTPUT_PATH: String = "./src/main/resources/output/parquet/"
}

object Constants extends Constants
