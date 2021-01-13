
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


def openCSV_toSQL(path,temp_table):
    data = spark.read.csv(path, header=True)
    data.registerTempTable(temp_table)


# In[20]:


resources = "gs://indra-misaint/examen_diagnostico_datio/src/main/resources/input/csv/comics/"
output_pth = "gs://indra-misaint/examen_diagnostico_datio/src/main/resources/output/"


# In[8]:


chrt_path = resources + 'characters.csv'
openCSV_toSQL(chrt_path,'characters')


# In[9]:


chrt_comic_path = resources + 'charactersToComics.csv'
openCSV_toSQL(chrt_comic_path,'charactersToComics')


# In[10]:


comic_path = resources + 'comics.csv'
openCSV_toSQL(comic_path,'comics')


# In[11]:


join_qry1 = """
              SELECT 
                  chr.characterID, chr.name, chrCom.comicID
              FROM 
                  characters AS chr
              INNER JOIN 
                  charactersToComics AS chrCom
              ON 
                  chr.characterID = chrCom.characterID
           """


# In[12]:


nme_list = spark.sql(join_qry1)
nme_list.registerTempTable("joinchar")
tmpdf = nme_list.groupBy("comicID").agg(F.collect_list("name")).withColumnRenamed("collect_list(name)", "name_arr")
tmpdf.registerTempTable("name_list")


# In[13]:


join_qry2 = """
              SELECT 
                  co.comicID, co.title, co.issueNumber, nml.name_arr ,co.description
              FROM 
                  comics AS co
              INNER JOIN 
                  name_list AS nml
              ON 
                  co.comicID = nml.comicID
           """


# In[14]:


final_table = spark.sql(join_qry2)



# In[15]:


final_table.repartition(1).write.mode("overwrite").parquet(output_pth+'parquet')

