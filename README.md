# Examen Técnico Spark

## Instrucciones
Realizar un fork de este repositorio a tu cuenta de github
Crear una rama que por nombre lleve tus iniciales
Realizar los ejercicios solicitados abajo
Realizar un Pull Request a la rama solution desde la rama en que haz realizado los ejercicios

## Ejercicio 1

Dado el archivo PokemonData.csv, leerlo como DataFrame (se recomienda el uso de RDDs para la lectura inicial)
1. Obtener el promedio de ataque(attack) y defensa(defense) agrupando por tipo de pokemon (type1) y generación (generation)
2. Filtrando por los tipos de pokemon(type1) fire y water es necesario calcular el promedio de cada una de las siguientes columnas: 
    sp_attack, sp_defense y speed; de tal forma que el dataframe resultante muestre los siguientes datos:
    generation, avg_sp_attack_water, avg_sp_attack_fire, avg_sp_defense_water, avg_sp_defense_fire, avg_speed_water, avg_speed_fire
    
## Ejercicio 2
Dado el archivo players_20.csvn nuestro coach Ramón necesita saber 
1. Cuales son los 20 jugadores de menos de 23 años que tienen más potencial
2. Cuales son los equiois top 20 con el promedio (overall) más alto
3. De los 20 equipos anteriores nuestro coach también quiere saber cuáles son los 3 porteros y los 3 delanteros con mejor (overall)
4. Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a los 5 mejores de cada país. 

## Ejercicio 3
Dados los archivos contenidos en la carpeta comis
1. Al Dataframe que contiene los nombres de comics queremos agregar una columna que contenga los personajes a forma de array


### Resultados

Cada uno de los Dataframes resultantes debe ser escrito en formato parquet en una carpeta dentro de src/main/output/parquet/
