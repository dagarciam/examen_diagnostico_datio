// For this exersice is necessary start spark-shell and execute this script

//import 

import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType;

//crete sqlContext vatiable
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._

//read file
val ds  =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/fifa/players_20.csv")

//print Schema of ds variable

//ds.printSchema()

// Register variable ds as Temporal table for using SQLContext

ds.registerTempTable("resultado")

// Query to consult : The best 20 players under 23 years

// sqlContext.sql("SELECT long_name, nationality, potential FROM resultado WHERE age < 23 ORDER BY potential DESC" ).show(20)

// Scala intrerpreter after query execution
/*
+--------------------+-----------+---------+
|           long_name|nationality|potential|
+--------------------+-----------+---------+
|       Kylian Mbappé|     France|       95|
| João Félix Sequeira|   Portugal|       93|
|    Matthijs de Ligt|Netherlands|       93|
|Gianluigi Donnarumma|      Italy|       92|
|Vinícius José de ...|     Brazil|       92|
|        Jadon Sancho|    England|       92|
|         Kai Havertz|    Germany|       92|
|     Frenkie de Jong|Netherlands|       91|
|     Ousmane Dembélé|     France|       90|
|        Philip Foden|    England|       90|
|          Alex Meret|      Italy|       89|
|Mikel Oyarzabal U...|      Spain|       89|
|          Luka Jović|     Serbia|       89|
|Tanguy NDombèlé A...|     France|       89|
|          Moise Kean|      Italy|       89|
|       Sandro Tonali|      Italy|       89|
|Trent Alexander-A...|    England|       89|
|Lucas Tolentino C...|     Brazil|       89|
|Rodrygo Silva de ...|     Brazil|       89|
|       Houssem Aouar|     France|       89|
+--------------------+-----------+---------+
only showing top 20 rows  */

// Save query as variable = more_pot

val  more_pot = sqlContext.sql("SELECT long_name, nationality, potential FROM resultado WHERE age < 23 ORDER BY potential DESC" )

// Write query result and save as Task2_ex1 in parquet format

more_pot.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Task2_ex1")

// Query to consult : Teams with the best average (overrall) Top 20

/* 

//sqlContext.sql("SELECT club, overall FROM resultado ORDER BY overall DESC" ).show(20)

//Scala interpreter after query execution

+-------------------+-------+
|               club|overall|
+-------------------+-------+
|       FC Barcelona|     94|
|           Juventus|     93|
|Paris Saint-Germain|     92|
|    Atlético Madrid|     91|
|    Manchester City|     91|
|        Real Madrid|     91|
|          Liverpool|     90|
|        Real Madrid|     90|
|       FC Barcelona|     90|
|          Liverpool|     90|
|          Liverpool|     89|
|             Napoli|     89|
|        Real Madrid|     89|
|Paris Saint-Germain|     89|
|    Manchester City|     89|
|            Chelsea|     89|
|  Tottenham Hotspur|     89|
|  Manchester United|     89|
|  FC Bayern München|     89|
|           Juventus|     89|
+-------------------+-------+
only showing top 20 rows */

// Save query as variable = top_20

val top_20 = sqlContext.sql("SELECT club, overall FROM resultado ORDER BY overall DESC" )

// Write query result and save as Task2_ex2 in parquet format

top_20.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Task2_ex2")


//Query to consult : The  best three GK of Top 20
/*
sqlContext.sql("SELECT short_name, club, team_position, overall FROM resultado WHERE team_position == 'GK' AND overall >=89 ORDER BY overall DESC" ).show(3)
+-------------+---------------+-------------+-------+
|   short_name|           club|team_position|overall|
+-------------+---------------+-------------+-------+
|     J. Oblak|Atlético Madrid|           GK|     91|
|M. ter Stegen|   FC Barcelona|           GK|     90|
|      Alisson|      Liverpool|           GK|     89|
+-------------+---------------+-------------+-------+
only showing top 3 rows
*/
// Save query as variable = best_GK

val best_GK = sqlContext.sql("SELECT short_name, club, team_position, overall FROM resultado WHERE team_position == 'GK' AND overall >=89 ORDER BY overall DESC" )

// Write query result and save as Task2_ex3 in parquet format

best_GK.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Task2_ex3")


//Query to consult : The  best three LW, ST or CF of Top 20
/*
sqlContext.sql("SELECT short_name, club, team_position, overall FROM resultado WHERE team_position == 'CF' OR team_position =='ST' OR team_position == 'LW'  AND overall >=89 ORDER BY overall DESC" ).show(3)
+-----------------+-----------------+-------------+-------+
|       short_name|             club|team_position|overall|
+-----------------+-----------------+-------------+-------+
|Cristiano Ronaldo|         Juventus|           LW|     93|
|        E. Hazard|      Real Madrid|           LW|     91|
|          H. Kane|Tottenham Hotspur|           ST|     89|
+-----------------+-----------------+-------------+-------+
only showing top 3 rows
*/
// Save query as variable = best_lw
 val best_lw = sqlContext.sql("SELECT short_name, club, team_position, overall FROM resultado WHERE team_position == 'CF' OR team_position =='ST' OR team_position == 'LW'  AND overall >=89 ORDER BY overall DESC" )

 // Write query result and save as Task2_ex3 in parquet format

 best_lw.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Task2_ex31")

 //Query to consult  Ranking of players 
 /*
 ds.sqlContext.sql("SELECT short_name, nationality, overall FROM resultado WHERE nationality = nationality ORDER BY overall DESC" ).show()

 +-----------------+-----------+-------+
|       short_name|nationality|overall|
+-----------------+-----------+-------+
|         L. Messi|  Argentina|     94|
|Cristiano Ronaldo|   Portugal|     93|
|        Neymar Jr|     Brazil|     92|
|         J. Oblak|   Slovenia|     91|
|     K. De Bruyne|    Belgium|     91|
|        E. Hazard|    Belgium|     91|
|         M. Salah|      Egypt|     90|
|        L. Modrić|    Croatia|     90|
|    M. ter Stegen|    Germany|     90|
|      V. van Dijk|Netherlands|     90|
|          Alisson|     Brazil|     89|
|     K. Koulibaly|    Senegal|     89|
|     Sergio Ramos|      Spain|     89|
|        K. Mbappé|     France|     89|
|        S. Agüero|  Argentina|     89|
|         N. Kanté|     France|     89|
|          H. Kane|    England|     89|
|           De Gea|      Spain|     89|
|   R. Lewandowski|     Poland|     89|
|     G. Chiellini|      Italy|     89|
+-----------------+-----------+-------+
only showing top 20 rows
*/
//Save Query as variable = ranking

val ranking = ds.sqlContext.sql("SELECT short_name, nationality, overall FROM resultado WHERE nationality = nationality ORDER BY overall DESC" )

// Write query result and save as Task2_ex4 in parquet format
ranking.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Task2_ex4")
