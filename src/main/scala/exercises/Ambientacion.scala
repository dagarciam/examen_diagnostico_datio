package exercises
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

  /** Ambientacion to reading csv.
   *
   *  @constructor create a new Ambientacion class to create dateframes to comics
   *  @param spark the spark sesion
   */
  class Ambientacion(spark: SparkSession) {


    /** Creates a Character Dataframe reading a csv
     */
    def createCharacter():DataFrame={
      def schemaCaracter = StructType(Array(
        StructField("characterID", StringType, true),
        StructField("name", StringType, true)))
      var csvCaracter = spark.read.schema(schemaCaracter).option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("delimiter", ",").csv("src/main/resources/input/csv/comics/characters.csv")
      csvCaracter.show()
      csvCaracter
    }

    /** Creates a comic Dataframe reading a csv
     */
    def createComic():DataFrame={
      def schemaComic = StructType(Array(StructField("comicID", StringType, true),
        StructField("title", StringType, true),
        StructField("issueNumber", StringType, true),
        StructField("description", StringType, true)
      ))
      var csvcomic = spark.read.schema(schemaComic).option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("delimiter", ",").csv("src/main/resources/input/csv/comics/comics.csv")
      csvcomic.show()
      csvcomic

    }
    /** Creates a Intermediate Dataframe reading a csv
     */
    def createIntermediate():DataFrame={
      def schemaIntermediate = StructType(Array(StructField("comicID", StringType, true),
        StructField("characterID", StringType, true)
      ))
      var csvIntermediate = spark.read.schema(schemaIntermediate).option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("delimiter", ",").csv("src/main/resources/input/csv/comics/charactersToComics.csv")
      csvIntermediate.show()
      csvIntermediate
    }

    /** Creates a Intermediate Dataframe reading a csv
     */
    def createPlayers():DataFrame={
      def schemaPlayers = StructType(Array(StructField("sofifa_id",StringType,true),
        StructField("player_url",StringType,true),
        StructField("short_name",StringType,true),
        StructField("long_name",StringType,true),
        StructField("age",StringType,true),
        StructField("dob",StringType,true),
        StructField("height_cm",StringType,true),
        StructField("weight_kg",StringType,true),
        StructField("nationality",StringType,true),
        StructField("club",StringType,true),
        StructField("overall",StringType,true),
        StructField("potential",StringType,true),
        StructField("value_eur",StringType,true),
        StructField("wage_eur",StringType,true),
        StructField("player_positions",StringType,true),
        StructField("preferred_foot",StringType,true),
        StructField("international_reputation",StringType,true),
        StructField("weak_foot",StringType,true),
        StructField("skill_moves",StringType,true),
        StructField("work_rate",StringType,true),
        StructField("body_type",StringType,true),
        StructField("real_face",StringType,true),
        StructField("release_clause_eur",StringType,true),
        StructField("player_tags",StringType,true),
        StructField("team_position",StringType,true),
        StructField("team_jersey_number",StringType,true),
        StructField("loaned_from",StringType,true),
        StructField("joined",StringType,true),
        StructField("contract_valid_until",StringType,true),
        StructField("nation_position",StringType,true),
        StructField("nation_jersey_number",StringType,true),
        StructField("pace",StringType,true),
        StructField("shooting",StringType,true),
        StructField("passing",StringType,true),
        StructField("dribbling",StringType,true),
        StructField("defending",StringType,true),
        StructField("physic",StringType,true),
        StructField("gk_diving",StringType,true),
        StructField("gk_handling",StringType,true),
        StructField("gk_kicking",StringType,true),
        StructField("gk_reflexes",StringType,true),
        StructField("gk_speed",StringType,true),
        StructField("gk_positioning",StringType,true),
        StructField("player_traits",StringType,true),
        StructField("attacking_crossing",StringType,true),
        StructField("attacking_finishing",StringType,true),
        StructField("attacking_heading_accuracy",StringType,true),
        StructField("attacking_short_passing",StringType,true),
        StructField("attacking_volleys",StringType,true),
        StructField("skill_dribbling",StringType,true),
        StructField("skill_curve",StringType,true),
        StructField("skill_fk_accuracy",StringType,true),
        StructField("skill_long_passing",StringType,true),
        StructField("skill_ball_control",StringType,true),
        StructField("movement_acceleration",StringType,true),
        StructField("movement_sprint_speed",StringType,true),
        StructField("movement_agility",StringType,true),
        StructField("movement_reactions",StringType,true),
        StructField("movement_balance",StringType,true),
        StructField("power_shot_power",StringType,true),
        StructField("power_jumping",StringType,true),
        StructField("power_stamina",StringType,true),
        StructField("power_strength",StringType,true),
        StructField("power_long_shots",StringType,true),
        StructField("mentality_aggression",StringType,true),
        StructField("mentality_interceptions",StringType,true),
        StructField("mentality_positioning",StringType,true),
        StructField("mentality_vision",StringType,true),
        StructField("mentality_penalties",StringType,true),
        StructField("mentality_composure",StringType,true),
        StructField("defending_marking",StringType,true),
        StructField("defending_standing_tackle",StringType,true),
        StructField("defending_sliding_tackle",StringType,true),
        StructField("goalkeeping_diving",StringType,true),
        StructField("goalkeeping_handling",StringType,true),
        StructField("goalkeeping_kicking",StringType,true),
        StructField("goalkeeping_positioning",StringType,true),
        StructField("goalkeeping_reflexes",StringType,true),
        StructField("ls",StringType,true),
        StructField("st",StringType,true),
        StructField("rs",StringType,true),
        StructField("lw",StringType,true),
        StructField("lf",StringType,true),
        StructField("cf",StringType,true),
        StructField("rf",StringType,true),
        StructField("rw",StringType,true),
        StructField("lam",StringType,true),
        StructField("cam",StringType,true),
        StructField("ram",StringType,true),
        StructField("lm",StringType,true),
        StructField("lcm",StringType,true),
        StructField("cm",StringType,true),
        StructField("rcm",StringType,true),
        StructField("rm",StringType,true),
        StructField("lwb",StringType,true),
        StructField("ldm",StringType,true),
        StructField("cdm",StringType,true),
        StructField("rdm",StringType,true),
        StructField("rwb",StringType,true),
        StructField("lb",StringType,true),
        StructField("lcb",StringType,true),
        StructField("cb",StringType,true),
        StructField("rcb",StringType,true),
        StructField("rb",StringType,true)))

      var csvPlayers = spark.read.schema(schemaPlayers).option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("delimiter", ",").csv("src/main/resources/input/csv/fifa/players_20.csv")
      csvPlayers.show()
      csvPlayers
    }

  }



