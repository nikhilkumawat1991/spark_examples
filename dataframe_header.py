#!/usr/bin/env python
# coding: utf-8

# In[12]:


import findspark
findspark.init()


# In[13]:


import pyspark
import random
from pyspark.sql import SQLContext
from pyspark.sql.types import  (StructType, 
                                StructField, 
                                DateType, 
                                BooleanType,
                                DoubleType,
                                IntegerType,
                                StringType,
                               TimestampType)


# In[14]:


emp_schema = StructType([StructField("ID", StringType(), True),
                            StructField("Name", StringType(), True),
                            StructField("age", StringType(), True ),
                            StructField("City", StringType(), True)
                            ])
schema_lst = ["Id","Name","age","city"]


# In[31]:


spark = pyspark.SparkContext.getOrCreate()
words = spark.textFile("/home/cgt_jpr_lin_nikhil/nikhil/spark/data/emp.txt")
print(type(words))
#print(words.getNumPartitions())
rdd_new = words.collect
print(type(rdd_new))


# In[16]:


# function to convert RDD to dataframe
def RDD_to_df(sqlContext,df,schema):
    
  # converting RDD to df using createDataframe()
  # in which we are passing RDD and schema of df
  df1 = sqlContext.createDataFrame(df,schema)
  return df1


# In[26]:


sqlContext = SQLContext(spark)
converted_df = RDD_to_df(sqlContext,words,emp_schema)
print(converted_df.first)


# In[24]:


print(type(words))
converted_df.show()


# In[ ]:





# In[ ]:





# In[ ]:




