package constants

trait Constants {
  val OPTION_HEADER : String = "header"
  val OPTION_DELIMITER : String = "delimiter"
  val OPTION_QUOTE : String = "quote"
  val INPUT_FORMAT : String = "csv"
  val INPUT_HEADER : Boolean = true
  val INPUT_DELIMITER : String = ","
  val INPUT_QUOTE : String = "\""
  val INPUT_PATH : String = "src/main/resources/input/csv/"
  val DIRECTORY_COMICS : String = "comics/"
  val DIRECTORY_FIFA : String = "fifa/"
  val DIRECTORY_POKEMON : String = "pokemon/"
  val COMICS_1 : String = "characters.csv"
  val COMICS_2 : String = "charactersToComics.csv"
  val COMICS_3 : String = "comics.csv"
  val FIFA_1 : String = "players_20.csv"
  val POKEMON_1 : String = "PokemonData.csv"
  val OUTPUT_FORMAT : String = "parquet"
  val OUTPUT_PATH : String = "src/main/output/parquet/"
  val OUTPUT_PATH_EJERCICIO_1 : String = "ejercicio1/"
  val OUTPUT_PATH_EJERCICIO_2A : String = "ejercicio2a/"
  val OUTPUT_PATH_EJERCICIO_2B : String = "ejercicio2b/"
  val OUTPUT_PATH_EJERCICIO_2C : String = "ejercicio2c/"
  val OUTPUT_PATH_EJERCICIO_2D : String = "ejercicio2d/"
  val OUTPUT_PATH_EJERCICIO_3 : String = "ejercicio3/"
}

object Constants extends Constants
