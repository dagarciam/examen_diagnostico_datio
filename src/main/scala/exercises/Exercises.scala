package exercises

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Exercises(spark: SparkSession) 
{
  //EJERCICIO 1
  //Al Dataframe que contiene los nombres de comics queremos agregar una columna que contenga los personajes a forma de array
  
  //leemos los csv para colocarlos en data frames
  val df_comics = spark
    .read
    .option("header","true")
    .csv("src/main/resources/input/csv/comics/comics.csv")
  val df_characters_to_comics = spark
    .read
    .option("header","true")
    .csv("src/main/resources/input/csv/comics/charactersToComics.csv")
  val df_characters = spark
    .read
    .option("header","true")
    .csv("src/main/resources/input/csv/comics/characters.csv")
  
  //hacemos join a characters_to_comics de characters para sacar sus nombres
  //var df = df_characters_to_comics.join(df_characters,df_characters_to_comics("characterID") ===  df_characters("characterID"),"leftouter").show(false)
  var comics_characters = df_characters_to_comics
    .join(df_characters, "characterID", "leftouter")

  //ya que tenemos el dataframe comics_characters hacemos la función de agregación para obtener la lista de personajes por comics
  var agg_comics_chars = comics_characters.groupBy("comicID").agg(collect_list("name"))
  
  //por último hacemos el join con el data frame de comics
  var final_comics_characters=agg_comics_chars.join(df_comics,"comicID","leftouter")
  
  var final_comics_characters_p=final_comics_characters.withColumnRenamed("collect_list(name)","personajes")
  final_comics_characters_p.write.parquet("Ejercicio_1.parquet")
  
  
  
 ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
  
  //EJERCICIO 2
  //Dado el archivo players_20.csvn nuestro coach Ramón necesita saber

  //Cuales son los 20 jugadores de menos de 23 años que tienen más potencial
  //Cuales son los equipos top 20 con el promedio (overall) más alto
  //De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con mejor (overall)
  //Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a los 5 mejores de cada país.
  
  //Primero pasamos el csv a data frame
  val df_players=spark.read.csv("players_20.csv",header="true")
  
  //consulta para saber top 20 jugadores de menos de 23 años con mayor potencial
  //filtramos el data frame para traer sólo jugadores menores de 23 años
  df_filtrado=df_players.filter(df_players("age")<23)
  
  //hacemos la consulta para ver el top 20
  df_filtrado.select(df_filtrado("sofifa_id"),df_filtrado("short_name"),df_filtrado("long_name"),df_filtrado("age"),df_filtrado("potential"]).orderBy(df_filtrado("potential"),ascending=False).limit(20).show()
  
//   +---------+-------------------+--------------------+---+---------+
//|sofifa_id|         short_name|           long_name|age|potential|
//+---------+-------------------+--------------------+---+---------+
//|   231747|          K. Mbappé|       Kylian Mbappé| 20|       95|
//|   242444|         João Félix| João Félix Sequeira| 19|       93|
//|   235243|         M. de Ligt|    Matthijs de Ligt| 19|       93|
//|   230621|      G. Donnarumma|Gianluigi Donnarumma| 20|       92|
//|   238794|       Vinícius Jr.|Vinícius José de ...| 18|       92|
//|   233049|          J. Sancho|        Jadon Sancho| 19|       92|
//|   235790|         K. Havertz|         Kai Havertz| 20|       92|
//|   228702|         F. de Jong|     Frenkie de Jong| 22|       91|
//|   237692|           P. Foden|        Philip Foden| 19|       90|
//|   231443|         O. Dembélé|     Ousmane Dembélé| 22|       90|
//|   243812|            Rodrygo|Rodrygo Silva de ...| 18|       89|
//|   230142|          Oyarzabal|Mikel Oyarzabal U...| 22|       89|
//|   231281|T. Alexander-Arnold|Trent Alexander-A...| 20|       89|
//|   234906|           H. Aouar|       Houssem Aouar| 21|       89|
//|   236610|            M. Kean|          Moise Kean| 19|       89|
//|   225116|           A. Meret|          Alex Meret| 22|       89|
//|   231478|        L. Martínez|    Lautaro Martínez| 21|       89|
//|   235569|        T. Ndombele|Tanguy NDombèlé A...| 22|       89|
//|   232432|           L. Jović|          Luka Jović| 21|       89|
//|   233927|      Lucas Paquetá|Lucas Tolentino C...| 21|       89|
//+---------+-------------------+--------------------+---+---------+                  
val ejercicio2= df_filtrado.select(df_filtrado["sofifa_id"],df_filtrado["short_name"],df_filtrado["long_name"],df_filtrado["age"],df_filtrado["potential"]).orderBy(df_filtrado["potential"],ascending=False).limit(20)
ejercicio2.write.parquet("Ejercicio_2_1.parquet")              
                     
                     
                     
//Cuales son los equipos top 20 con el promedio (overall) más alto                     
// primero vamos a agrupar la información por clubes y por promedio del promedio de jugadores
var agg_equipos = df_players.groupBy("club").agg(avg("overall"))

//ahora hacemos la consulta del top 20 por promedios                     
agg_equipos.select(agg_equipos("club"),agg_equipos("avg(overall)").orderBy(agg_equipos("avg(overall)"),ascending=False).limit(20).show()                     
  
//  +-------------------+-----------------+
//|               club|     avg(overall)|
//+-------------------+-----------------+
//|  FC Bayern München|81.30434782608695|
//|        Real Madrid|80.12121212121212|
//|           Juventus|80.06060606060606|
//|            Uruguay| 78.6086956521739|
//|       FC Barcelona|78.36363636363636|
//|             Mexico|             78.0|
//|           Colombia|             78.0|
//|        Netherlands|             78.0|
//|Bayer 04 Leverkusen|            77.28|
//|            Chelsea|77.06060606060606|
//|    Manchester City|             77.0|
//|             Napoli|76.87096774193549|
//|  Manchester United|76.84848484848484|
//|  Tottenham Hotspur|76.48484848484848|
//|    Atlético Madrid|76.18181818181819|
//|              Milan|76.17241379310344|
//|             Turkey|             76.0|
//|              Lazio|75.93939393939394|
//|Paris Saint-Germain| 75.9090909090909|
//|          Liverpool|75.84848484848484|
//+-------------------+-----------------+                 
   
val Ejercicio_2_2= agg_equipos.withColumnRenamed("avg(overall)","average")
Ejercicio_2_2.write.parquet("Ejercicio_2_2.parquet")                   
                   
                   
//guardamos la consulta en un dataframe para el ejercicio siguiente                   
val Ejercicio_2_2=agg_equipos.withColumnRenamed("avg(overall)","average")                  
               
//De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con mejor (overall)                   
//hacemos el inner join del dataframe general con el del top 20 para traer sólo información de los top 20
val df_fitrado=df_players.join(Ejercicio_2_2,"club","inner")                   
                   
//hacemos el top 3 de porteros con mejor overall y el top 3 de los delanteros con mejor overall                   
val ejecricio_2_3_porteros= df_fitrado.select(df_fitrado("sofifa_id"),df_fitrado("short_name"),df_fitrado("long_name"),df_fitrado("club"),df_fitrado("player_positions"),df_fitrado("overall")).filter(df_fitrado("player_positions")=="GK").orderBy(df_fitrado("overall"),ascending=False).limit(3)                  

//+---------+-------------+--------------------+---------------+----------------+-------+
//|sofifa_id|   short_name|           long_name|           club|player_positions|overall|
//+---------+-------------+--------------------+---------------+----------------+-------+
//|   200389|     J. Oblak|           Jan Oblak|Atlético Madrid|              GK|     91|
//|   192448|M. ter Stegen|Marc-André ter St...|   FC Barcelona|              GK|     90|
//|   212831|      Alisson|Alisson Ramses Be...|      Liverpool|              GK|     89|
//+---------+-------------+--------------------+---------------+----------------+-------+                  
 ejecricio_2_3_porteros.write.parquet("Ejercicio_2_3_porteros.parquet")
                         
                   
val ejecricio_2_3_delanteros= df_fitrado.select(df_fitrado("sofifa_id"),df_fitrado("short_name"),df_fitrado("long_name"),df_fitrado("club"),df_fitrado("player_positions"),df_fitrado("overall")).filter(df_fitrado.player_positions.like("%CF%")).orderBy(df_fitrado("overall"),ascending=False).limit(3)               

//+---------+------------+--------------------+------------+----------------+-------+
//|sofifa_id|  short_name|           long_name|        club|player_positions|overall|
//+---------+------------+--------------------+------------+----------------+-------+
//|   158023|    L. Messi|Lionel Andrés Mes...|FC Barcelona|      RW, CF, ST|     94|
//|   183277|   E. Hazard|         Eden Hazard| Real Madrid|          LW, CF|     91|
//|   194765|A. Griezmann|   Antoine Griezmann|FC Barcelona|      CF, ST, LW|     89|
//+---------+------------+--------------------+------------+----------------+-------+                   
ejecricio_2_3_delanteros.write.parquet("ejecricio_2_3_delanteros.parquet")
                   
                   
                   
 //Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a los 5 mejores de cada país.     
  window = Window.partitionBy(df_players['nationality']).orderBy(df_players['overall'].desc())
  val ejercicio_2_4= df_players.select(df_players["nationality"],df_players["sofifa_id"],df_players["overall"],df_players["short_name"],df_players["long_name"], rank().over(window).alias('rank')) .filter(col('rank') <= 6) .show()
                   
                   
 
                   
                   
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 
//EJERCICIO 3
//Dado el archivo PokemonData.csv, leerlo como DataFrame (se recomienda el uso de RDDs para la lectura inicial)
//Filtrando por los tipos de pokemon(type1) fire y water es necesario calcular el promedio de cada una de las siguientes columnas: 
//sp_attack, sp_defense y speed; de tal forma que el dataframe resultante muestre los siguientes datos: generation, avg_sp_attack_water, avg_sp_attack_fire, 
//avg_sp_defense_water, avg_sp_defense_fire, avg_speed_water, avg_speed_fire                   
                   
                   
  //leemos el csv y lo pasamos a dataframe                 
   val df_pokemones=spark.read.csv("PokemonData.csv",header="true")                 
 
  //creamos 2 data frames para lso 2 tipos de pokemones                
   val df_pokemones_fuego=df_pokemones.select(df_pokemones_fuego("generation"),df_pokemones_fuego("sp_attack"),df_pokemones_fuego("sp_defense"),df_pokemones_fuego("speed")).filter(df_pokemones("type1")=="fire")
   val df_pokemones_agua=df_pokemones.select(df_pokemones_agua("generation"),df_pokemones_agua("sp_attack"),df_pokemones_agua("sp_defense"),df_pokemones_agua("speed")).filter(df_pokemones("type1")=="water")
                   
   //calculamos los promedios de sp_attack, sp_defense y speed por generación para ambos dataframe                
    val df_pokemones_fuego_promedios= df_pokemones_fuego.groupBy("generation").agg(avg("sp_attack") ,avg("sp_defense"),avg("speed"))
    val df_pokemones_fuego_final=df_pokemones_fuego_promedios.withColumnRenamed("avg(sp_attack)", "avg_sp_attack_fire").withColumnRenamed("avg(sp_defense)", "avg_sp_defense_fire").withColumnRenamed("avg(speed)", "avg_speed_fire")               
      
//     +----------+------------------+-------------------+------------------+
//|generation|avg_sp_attack_fire|avg_sp_defense_fire|    avg_speed_fire|
//+----------+------------------+-------------------+------------------+
//|         1|             88.75|  79.16666666666667|             84.75|
//|         2|            84.875|               75.5|              71.0|
//|         3| 96.66666666666667|  68.33333333333333|45.833333333333336|
//|         4|              99.0|               73.6|              82.0|
//|         5|             78.25|               61.5|              62.5|
//|         6|              88.5|             70.125|            86.875|
//|         7|              81.8|               67.0|              69.8|
//+----------+------------------+-------------------+------------------+              
                                
    val df_pokemones_agua_promedios= df_pokemones_agua.groupBy("generation").agg(avg("sp_attack") ,avg("sp_defense"),avg("speed"))
    val df_pokemones_agua_final=df_pokemones_agua_promedios.withColumnRenamed("avg(sp_attack)", "avg_sp_attack_water").withColumnRenamed("avg(sp_defense)", "avg_sp_defense_water").withColumnRenamed("avg(speed)", "avg_speed_water")             
// +----------+-------------------+--------------------+------------------+
//|generation|avg_sp_attack_water|avg_sp_defense_water|   avg_speed_water|
//+----------+-------------------+--------------------+------------------+
//|         1|  68.03571428571429|   67.82142857142857| 67.71428571428571|
//|         2|  68.27777777777777|   75.88888888888889|56.833333333333336|
//|         3|  74.95833333333333|   66.33333333333333|62.458333333333336|
//|         4|  81.15384615384616|   78.76923076923077|              70.0|
//|         5|  74.76470588235294|  62.294117647058826| 66.17647058823529|
//|         6|               95.2|                64.6|              80.6|
//|         7|  78.66666666666667|  102.44444444444444|47.888888888888886|
//+----------+-------------------+--------------------+------------------+                  
                   
 
  //creamos el data frame final con los datos de ambos dataframes anteriores                 
  val generation_dataframe=df_pokemones_fuego_final.join(df_pokemones_agua_final,"generation","inner")
  generation_dataframe.write.parquet("ejercicio_3.parquet")                 
                   
}
