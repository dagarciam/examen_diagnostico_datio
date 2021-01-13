
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


# In[3]:


from pyspark.conf import SparkConf


# In[4]:


from pyspark.sql.session import SparkSession


# In[5]:

sc = SparkContext()
spark = SQLContext(sc)


# In[6]:


resources = "gs://indra-misaint/examen_diagnostico_datio/src/main/resources/input/csv/fifa/players_20.csv"
output_pth = "gs://indra-misaint/examen_diagnostico_datio/src/main/resources/output/parquet/"


# In[7]:


data = spark.read.csv(resources, header=True)
data.registerTempTable("players")


# In[8]:


def saveParquet(df,name):
    df.repartition(1).write.mode("overwrite").parquet(output_pth+name)


# In[9]:

# Cuales son los 20 jugadores de menos de 23 años que tienen más potencial

qry1 = """
        SELECT 
            long_name, potential
        FROM
            players
        WHERE
            age > 23
        ORDER BY
            potential DESC
        LIMIT 
            20
"""


# In[10]:


play_ovr = spark.sql(qry1)
saveParquet(play_ovr,"Overal_Players")


# In[11]:

# Cuales son los equipos top 20 con el promedio (overall) más alto

qry2 = """
        SELECT 
            club ,ROUND(AVG(overall),2) AS AVG_Overall
        FROM
            players
        WHERE
            nationality <> club
        GROUP BY
            club
        ORDER BY
            AVG(overall) DESC
        LIMIT 
            20
"""


# In[12]:


team_ovr = spark.sql(qry2)
saveParquet(team_ovr,"Overal_Team")


# In[13]:


team_ovr.registerTempTable("team_ovr")
temp_tbl = spark.sql("SELECT * FROM team_ovr")


# In[14]:

# De los 20 equipos anteriores nuestro coach también quiere saber cuáles 
# son los 3 porteros y los 3 delanteros con mejor (overall)

qry3 = """
        SELECT 
            club, long_name, player_positions ,overall
        FROM
            players
        WHERE
            club
        IN
            (
               SELECT 
                    club 
               FROM 
                    team_ovr
            )
        ORDER BY
            overall DESC
"""


# In[15]:


pl_bsteam = spark.sql(qry3)
pl_bsteam.registerTempTable("pl_bsteam")


# In[16]:


qry4 = """
        SELECT 
            *
        FROM
            pl_bsteam
        WHERE
            player_positions = 'GK'
        ORDER BY
            overall DESC
        LIMIT 
            3
"""


# In[17]:


best_gk = spark.sql(qry4)
best_gk.registerTempTable("best_gk")


# In[18]:


pl_spt_pos = pl_bsteam.select('club','long_name','overall', F.split('player_positions', ', ').alias('player_positions'))
pl_spt_pos_sizes = pl_spt_pos.select(F.size('player_positions').alias('player_positions'))
pl_spt_pos_max = pl_spt_pos_sizes.agg(F.max('player_positions'))
nb_columns = pl_spt_pos_max.collect()[0][0]
pl_spt_pos_result = pl_spt_pos.select('club','long_name','overall', *[pl_spt_pos['player_positions'][i] for i in range(nb_columns)])
pl_spt_pos_result.registerTempTable("pl_spt_pos_result")


# In[19]:


qry5 = """
            SELECT 
                *
            FROM
                pl_bsteam
            WHERE
                long_name 
            IN (
                 SELECT 
                     long_name
                 FROM 
                     pl_spt_pos_result
                 WHERE 
                        `player_positions[0]` IN ('RW','LW','CF','ST') 
                     OR `player_positions[1]` IN ('RW','LW','CF','ST') 
                     OR `player_positions[2]` IN ('RW','LW','CF','ST') 
                 ORDER BY
                     overall DESC
                 LIMIT 3)
            ORDER BY
                overall DESC
             
"""


# In[20]:


bst_del = spark.sql(qry5)
final_bst_ply = bst_del.union(best_gk)
saveParquet(final_bst_ply,"Best_GK_DEL")


# In[21]:

# Rankear a los jugadores por nacionalidad de tal forma que identifiquemos a 
# los 5 mejores de cada país.

qr6 = """
          SELECT 
              *
          FROM (
                 SELECT nationality,long_name, overall,
                 ROW_NUMBER() 
                 OVER 
                     (PARTITION BY 
                           nationality 
                      ORDER BY overall DESC) 
                 AS rn
                 FROM players
                )
"""


# In[22]:


all_cnt = spark.sql(qr6)
all_cnt.registerTempTable("all_cnt")


# In[23]:


qr7 = """
          SELECT 
              * 
          FROM
              all_cnt 
          WHERE 
              rn 
          BETWEEN 
              1 AND 5 
          ORDER BY 
              nationality ASC, overall DESC
"""


# In[24]:


best_five_nat = spark.sql(qr7)
saveParquet(best_five_nat,"Best_Five_Nat")

