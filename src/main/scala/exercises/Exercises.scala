package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, sum}

object Exercises {
  def main(args: Array[String]): Unit = {
    //Los resultados estan en un spark-shell ya que cuento con acceso limitado a internet por ahora.
    //Pero el cogigo es funcional y los resultados son los esperados.
    val spark = SparkSession.builder.master("local[*]").appName("Ejercicio1").getOrCreate()
    val grupoDeJugadores = spark.read.format("csv").option("header","true").load("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/fifa/players_20.csv")

    //Ejercicio 1
    grupoDeJugadores.filter("age < '23'")
                    .orderBy(desc("potential"))
                    .repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema1.parquet")

    //Ejercicio 2
    grupoDeJugadores.selectExpr("club", "cast(overall as Int) as promedio")
                    .groupBy("club")
                    .sum("promedio")
                    .orderBy(sum("promedio").desc)
                    .selectExpr("club", "`sum(promedio)` as promedio").repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema2.parquet")
   //Ejercicio 3
    grupoDeJugadores.createOrReplaceTempView("EquiposFutbol")
    spark.sql("select long_name, player_positions, overall from EquiposFutbol where club in ('Real Madrid','Juventus','FC Barcelona','Chelsea','Manchester City','Manchester United','Tottenham Hotspur','Atlético Madrid','Lazio','Paris Saint-Germain','Liverpool','Sevilla FC','Valencia CF','Everton','Arsenal','Watford','Leicester City','RB Leipzig','Athletic Club de Bilbao','Eintracht Frankfurt') and player_positions in ('GK', 'ST') group By long_name, player_positions, overall having count(1) <=3 order by overall desc").repartition(1).write.parquet("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/Ejercicio2_problema3.parquet")


  }
}
