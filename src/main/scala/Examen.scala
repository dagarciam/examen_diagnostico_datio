import constants.Constants

object Examen {
  def main(args: Array[String]): Unit = {
    val comics = spark.sparkContext.textFile("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/comics/comics.csv")
    val bitacora_comic_con_personaje = spark.sparkContext.textFile("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/comics/charactersToComics.csv").filter(x => sonDigitos(x.split(",")(0))).map(x => (x.split(",")(1), x ) )
    val personajes = spark.sparkContext.textFile("file:///home/bashroot/Proyectos/examen_diagnostico_datio/src/main/resources/input/csv/comics/characters.csv").filter(x => sonDigitos(x.split(",")(0))).map(x => (x.split(",")(0), x ) )



    val header = comics.first
    def sonDigitos(cadena: String) = cadena forall Character.isDigit
    val comics_data_limpio = comics.filter(x => x!= header)
                                   .filter(x => x.split(",").size == 4)
                                   .map(x => ( x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3))).toDF("comicID","title","issueNumber","description")

    val join_bitacora_personajes = bitacora_comic_con_personaje.join(personajes)
    val join_personajes = join_bitacora_personajes.map(x => (x._2._1, x._2._2))
                                                  .map(x => (x._1.split(",")(0),( x._2.split(",")(1)) ))

    val grupoDePersonajes = join_personajes.toDF("id_character", "personaje")
                                           .withColumn("personajes", concat_ws(",", $"personaje"))
                                           .select("id_character", "personajes").groupBy("id_character").agg(concat_ws(",", collect_list("personajes")).as("personajes"))


    val resultado = comics_data_limpio.join(grupoDePersonajes, comics_data_limpio("comicID") === grupoDePersonajes("id_character")).select("comicID", "title", "issueNumber", "description", "personajes")
    resultado.repartition(1).write.parquet("/home/bashroot/Proyectos/examen_diagnostico_datio/src/main/output/parquet/comics_con_personajes.parquet")



    println(Constants.MESSAGE)
  }
}
