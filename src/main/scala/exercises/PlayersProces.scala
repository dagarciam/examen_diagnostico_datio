package exercises
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, when}

class PlayersProces(spark: SparkSession) {

def process23(players:DataFrame): DataFrame = {

    val windowSpec  = Window.partitionBy("nationality").orderBy("age")


    players.select(col("sofifa_id"),col("player_url"),col("short_name"),col("long_name"),col("age"),
      col("dob"),col("height_cm"),col("weight_kg"),col("nationality"),col("club"),
      col("overall"),col("potential"),col("value_eur"),col("wage_eur"),col("player_positions")
      ,col("preferred_foot"),col("international_reputation"),col("weak_foot"),col("skill_moves"),
      col("work_rate"),col("body_type"),col("real_face"),col("release_clause_eur"),col("player_tags"),
      col("team_position"),col("team_jersey_number"),col("loaned_from"),col("joined"),col("contract_valid_until"),
      col("nation_position"),col("nation_jersey_number"),col("pace"),col("shooting"),col("passing"),
      col("dribbling"),col("defending"),col("physic"),col("gk_diving"),col("gk_handling"),col("gk_kicking"),
      col("gk_reflexes"),col("gk_speed"),col("gk_positioning"),col("player_traits"),col("attacking_crossing"),
      col("attacking_finishing"),col("attacking_heading_accuracy"),col("attacking_short_passing"),col("attacking_volleys"),col("skill_dribbling"),col("skill_curve"),
      col("skill_fk_accuracy"),col("skill_long_passing"),col("skill_ball_control"),col("movement_acceleration"),col("movement_sprint_speed"),
      col("movement_agility"),col("movement_reactions"),col("movement_balance"),col("power_shot_power"),col("power_jumping"),col("power_stamina"),
      col("power_strength"),col("power_long_shots"),col("mentality_aggression"),col("mentality_interceptions"),col("mentality_positioning"),col("mentality_vision"),col("mentality_penalties"),col("mentality_composure")
      ,col("defending_marking"),col("defending_standing_tackle"),col("defending_sliding_tackle"),col("goalkeeping_diving"),col("goalkeeping_handling"),col("goalkeeping_kicking"),col("goalkeeping_positioning"),col("goalkeeping_reflexes"),col("ls"),col("st"),col("rs"),
      col("lw"),col("lf"),col("cf"),col("rf"),col("rw"),col("lam"),col("cam"),col("ram"),col("lm"),col("lcm"),
      col("cm"),col("rcm"),col("rm"),col("lwb"),col("ldm"),col("cdm"),col("rdm"),
      col("rwb"),col("lb"),col("lcb"),col("cb"),col("rcb"),col("rb"),
      row_number().over(windowSpec).as("Row"),
      when(row_number().over(windowSpec).as("Row").<("20").and(col("age").<("23")),true).otherwise(false).as("top_20_under_23")

    ).orderBy(desc("potential"))

  }


  def processNationality(players:DataFrame): DataFrame = {

    val windowSpec  = Window.partitionBy("nationality").orderBy("overall")


    players.select(col("sofifa_id"),col("player_url"),col("short_name"),col("long_name"),col("age"),
      col("dob"),col("height_cm"),col("weight_kg"),col("nationality"),col("club"),
      col("overall"),col("potential"),col("value_eur"),col("wage_eur"),col("player_positions")
      ,col("preferred_foot"),col("international_reputation"),col("weak_foot"),col("skill_moves"),
      col("work_rate"),col("body_type"),col("real_face"),col("release_clause_eur"),col("player_tags"),
      col("team_position"),col("team_jersey_number"),col("loaned_from"),col("joined"),col("contract_valid_until"),
      col("nation_position"),col("nation_jersey_number"),col("pace"),col("shooting"),col("passing"),
      col("dribbling"),col("defending"),col("physic"),col("gk_diving"),col("gk_handling"),col("gk_kicking"),
      col("gk_reflexes"),col("gk_speed"),col("gk_positioning"),col("player_traits"),col("attacking_crossing"),
      col("attacking_finishing"),col("attacking_heading_accuracy"),col("attacking_short_passing"),col("attacking_volleys"),col("skill_dribbling"),col("skill_curve"),
      col("skill_fk_accuracy"),col("skill_long_passing"),col("skill_ball_control"),col("movement_acceleration"),col("movement_sprint_speed"),
      col("movement_agility"),col("movement_reactions"),col("movement_balance"),col("power_shot_power"),col("power_jumping"),col("power_stamina"),
      col("power_strength"),col("power_long_shots"),col("mentality_aggression"),col("mentality_interceptions"),col("mentality_positioning"),col("mentality_vision"),col("mentality_penalties"),col("mentality_composure")
      ,col("defending_marking"),col("defending_standing_tackle"),col("defending_sliding_tackle"),col("goalkeeping_diving"),col("goalkeeping_handling"),col("goalkeeping_kicking"),col("goalkeeping_positioning"),col("goalkeeping_reflexes"),col("ls"),col("st"),col("rs"),
      col("lw"),col("lf"),col("cf"),col("rf"),col("rw"),col("lam"),col("cam"),col("ram"),col("lm"),col("lcm"),
      col("cm"),col("rcm"),col("rm"),col("lwb"),col("ldm"),col("cdm"),col("rdm"),
      col("rwb"),col("lb"),col("lcb"),col("cb"),col("rcb"),col("rb"),
      row_number().over(windowSpec).as("Row"),
      col("top_20_under_23"),
      when(row_number().over(windowSpec).as("Row").<("16"),true).otherwise(false).as("top_15_by_nationality")

    ).orderBy(desc("overall"))

  }

  def processAVG(players:DataFrame): DataFrame = {

    val windowSpec  = Window.partitionBy("nationality").orderBy("nationality")


    players.select(col("sofifa_id"),col("player_url"),col("short_name"),col("long_name"),col("age"),
      col("dob"),col("height_cm"),col("weight_kg"),col("nationality"),col("club"),
      col("overall"),col("potential"),col("value_eur"),col("wage_eur"),col("player_positions")
      ,col("preferred_foot"),col("international_reputation"),col("weak_foot"),col("skill_moves"),
      col("work_rate"),col("body_type"),col("real_face"),col("release_clause_eur"),col("player_tags"),
      col("team_position"),col("team_jersey_number"),col("loaned_from"),col("joined"),col("contract_valid_until"),
      col("nation_position"),col("nation_jersey_number"),col("pace"),col("shooting"),col("passing"),
      col("dribbling"),col("defending"),col("physic"),col("gk_diving"),col("gk_handling"),col("gk_kicking"),
      col("gk_reflexes"),col("gk_speed"),col("gk_positioning"),col("player_traits"),col("attacking_crossing"),
      col("attacking_finishing"),col("attacking_heading_accuracy"),col("attacking_short_passing"),col("attacking_volleys"),col("skill_dribbling"),col("skill_curve"),
      col("skill_fk_accuracy"),col("skill_long_passing"),col("skill_ball_control"),col("movement_acceleration"),col("movement_sprint_speed"),
      col("movement_agility"),col("movement_reactions"),col("movement_balance"),col("power_shot_power"),col("power_jumping"),col("power_stamina"),
      col("power_strength"),col("power_long_shots"),col("mentality_aggression"),col("mentality_interceptions"),col("mentality_positioning"),col("mentality_vision"),col("mentality_penalties"),col("mentality_composure")
      ,col("defending_marking"),col("defending_standing_tackle"),col("defending_sliding_tackle"),col("goalkeeping_diving"),col("goalkeeping_handling"),col("goalkeeping_kicking"),col("goalkeeping_positioning"),col("goalkeeping_reflexes"),col("ls"),col("st"),col("rs"),
      col("lw"),col("lf"),col("cf"),col("rf"),col("rw"),col("lam"),col("cam"),col("ram"),col("lm"),col("lcm"),
      col("cm"),col("rcm"),col("rm"),col("lwb"),col("ldm"),col("cdm"),col("rdm"),
      col("rwb"),col("lb"),col("lcb"),col("cb"),col("rcb"),col("rb"),
      row_number().over(windowSpec).as("Row"),
      col("top_20_under_23"),
      col("top_15_by_nationality"),
      col("weight_kg").divide((col("height_cm").divide(100)).*(col("height_cm"))).as("IMC")
    )

  }


}
