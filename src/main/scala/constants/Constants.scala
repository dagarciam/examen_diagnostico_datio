package constants

trait Constants {
  val MESSAGE: String = "Hola mundo"

  val PATH_COMICS: String = "src/main/resources/input/csv/comics/comics.csv"
  val PATH_CHARACTER_TO_COMICS: String = "src/main/resources/input/csv/comics/characters.csv"
  val PATH_CHARACTERS: String =  "src/main/resources/input/csv/comics/charactersToComics.csv"
  val PATH_PLAYERS: String = "src/main/resources/input/csv/fifa/players_20.csv"
  val PATH_POKEMON: String = "src/main/resources/input/csv/pokemon/PokemonData.csv"


  val PATH_OUTPUT: String =  "src/main/output/parquet/"

  val COL_COMIC_ID: String = "comicID"
  val COL_CHARACTER_ID: String = "characterID"
  val COL_TITLE: String = "title"
  val COL_ISSUE_NUMBER: String = "issueNumber"
  val COL_DESCRIPTION: String = "description"
  val COL_NAME: String = "name"
  val COL_CHARACTER_NAME: String = "character_name"

  val COL_POTENTIAL: String = "potential"
  val COL_AGE: String = "age"
  val COL_CLUB: String = "club"
  val COL_OVERALL: String = "overall"
  val COL_MAX_OVERALL: String = "max_overall"
  val COL_PLAYER_POSITIONS: String = "player_positions"
  val COL_SHORT_NAME: String = "short_name"
  val COL_NATIONALITY: String = "nationality"
  val COL_RANKING: String = "ranking"

  val COL_TYPE1: String = "type1"
  val VAL_FIRE: String = "fire"
  val VAL_WATER: String = "water"
  val COL_GENERATION: String = "generation"
  val COL_SP_ATTACK: String = "sp_attack"
  val COL_SP_DEFENSE: String = "sp_defense"
  val COL_SPEED: String = "speed"

  val COL_AVG_SP_ATTACK_WATER: String = "water_avg(sp_attack)"
  val COL_AVG_SP_ATTACK_FIRE: String= "fire_avg(sp_attack)"
  val COL_AVG_SP_DEFENSE_WATER: String= "water_avg(sp_defense)"
  val COL_AVG_SP_DEFENSE_FIRE: String= "fire_avg(sp_defense)"
  val COL_AVG_SPEED_WATER: String= "water_avg(speed)"
  val COL_AVG_SPEED_FIRE: String= "fire_avg(speed)"

  val ALIAS_AVG_SP_ATTACK_WATER: String = "avg_sp_attack_water"
  val ALIAS_AVG_SP_ATTACK_FIRE: String= "avg_sp_attack_fire"
  val ALIAS_AVG_SP_DEFENSE_WATER: String= "avg_sp_defense_water"
  val ALIAS_AVG_SP_DEFENSE_FIRE: String= "avg_sp_defense_fire"
  val ALIAS_AVG_SPEED_WATER: String= "avg_speed_water"
  val ALIAS_AVG_SPEED_FIRE: String= "avg_speed_fire"




  val INNER_JOIN: String = "inner"
}

object Constants extends Constants
