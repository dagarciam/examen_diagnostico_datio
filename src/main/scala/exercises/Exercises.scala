package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, sum}

object Exercises {
  def main(args: Array[String]): Unit = {
    //Los resultados estan en un spark-shell ya que cuento con acceso limitado a internet por ahora.
    //Pero el cogigo es funcional y los resultados son los esperados.
    val spark = SparkSession.builder.master("local[*]").appName("Ejercicio1").getOrCreate()
    val grupoDeJugadores = spark.read.format("csv").option("header","true").load("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/fifa/players_20.csv")

    //Ejercicio 2 parte 1
    grupoDeJugadores.filter("age < '23'")
                    .orderBy(desc("potential"))
                    .repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema1.parquet")

    //Ejercicio 2 parte 2
    grupoDeJugadores.selectExpr("club", "cast(overall as Int) as promedio")
                    .groupBy("club")
                    .sum("promedio")
                    .orderBy(sum("promedio").desc)
                    .selectExpr("club", "`sum(promedio)` as promedio").repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema2.parquet")
   //Ejercicio 2_parte 3
    grupoDeJugadores.createOrReplaceTempView("EquiposFutbol")
    spark.sql("select long_name, player_positions, overall from EquiposFutbol where club in ('Real Madrid','Juventus','FC Barcelona','Chelsea','Manchester City','Manchester United','Tottenham Hotspur','Atlético Madrid','Lazio','Paris Saint-Germain','Liverpool','Sevilla FC','Valencia CF','Everton','Arsenal','Watford','Leicester City','RB Leipzig','Athletic Club de Bilbao','Eintracht Frankfurt') and player_positions in ('GK', 'ST') group By long_name, player_positions, overall having count(1) <=3 order by overall desc").repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema3.parquet")


    //Ejericio parte 3
    val pokemones = spark.read.format("csv")
      .option("header", "true")
      .load("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/pokemon/PokemonData.csv")


    pokemones.createOrReplaceTempView("pokemones")
    spark.sql("select generation, sp_attack, sp_defense, speed, avg(cast(sp_attack as double)) as avg_sp_attack_water, avg(cast(sp_attack as double)) as avg_sp_attack_fire, avg(cast(sp_defense as double)) as avg_sp_defense, avg(cast(sp_defense as double)) as avg_sp_defense_fire, avg(cast(speed as double)) as avg_speed_water, avg(cast(speed as double)) as avg_speed_fire from pokemones where type1 ='fire' or type1 = 'water' group by generation, sp_attack, sp_defense, speed").repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio3.parquet")
  }
}
