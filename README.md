# Examen Técnico Spark

## Instrucciones
1. Realizar un fork de este repositorio a tu cuenta de github
2. Crear una rama que por nombre lleve tus iniciales
3. Realizar los ejercicios solicitados abajo
4. Realizar un Pull Request a la rama solution desde la rama en que haz realizado los ejercicios

## ¿Qué evaluaremos?
* Haz uso de todos los recursos de POO qué estén a tu alcance, Herencia, Polimorfismo, Encapsulamiento, Clases abstractas, Traits, etc.
* Minimiza todo lo posible el shuffling.
Nuestra area de QA está traumada con el uso de withColumn, por favor reemplace el uso de este método con su imaginación.
* El uso de sentencias SQL queda estrictamente prohibido.
* El uso de cadenas en las clases que implementan la lógica de solución están muy mal vistos por nuestra area de QA, sea cuidadoso.
* Todos los métodos deben contener comentarios en estilo ScalaDoc.
* Amamos las pruebas de calidad, la implementación de pruebas unitarias al 30% de los métodos implementados nos hará muy felices.


## Ejercicio 1
Dados los archivos contenidos en la carpeta comics
1. Al Dataframe que contiene los nombres de comics queremos agregar una columna que contenga los personajes a forma de array

Escriba el dataframe resultante en la carpeta src/main/output/parquet/e1 en formato parquet

## Ejercicio 2
Dado el archivo players_20.csvn nuestro coach Ramón necesita saber
1. Agregar una columna top_20_under_23 de tipo BooleanType que contenga TRUE para los 20 jugadores de menos de 23 años que tienen más potencial y FALSE para el resto.
2. Agregar una columna top_15_by_nationality de typo BooleanType que contenga true si el jugador es uno de los mejores jugadores de su país (basados en la columna overall).
3. Agregar una columna qué contenga el promedio del IMC por cada nacionalidad
4. Agregar una columna culbs_top qué identifique si el jugador pertenece a uno de los mejores 20 clubes del mundo (promedio de la columna overall)
5. De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con overall más alto,
   para ello debes agregar la columna top_by_position de tipo IntegerType que contendrá el rank 1,2 o 3 para los 3 mejores y null para el resto

Escriba el Dataframe resultante particionado por nacionalidad en la carpeta src/main/output/parquet/e2 en formato parquet

## Ejercicio 3

Dado el archivo PokemonData.csv, leerlo como DataFrame (se recomienda el uso de RDDs para la lectura inicial)
1. Filtrando por los tipos de pokemon(type1) fire y water es necesario calcular el promedio de cada una de las siguientes columnas:
   sp_attack, sp_defense y speed; de tal forma que el dataframe resultante muestre los siguientes datos:
   generation, avg_sp_attack_water, avg_sp_attack_fire, avg_sp_defense_water, avg_sp_defense_fire, avg_speed_water, avg_speed_fire

Escriba el Dataframe resultante en la carpeta src/main/output/parquet/e3 en formato parquet