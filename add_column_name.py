#!/usr/bin/env python
# coding: utf-8

# In[2]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType


# In[3]:


appName = "PySpark Example - Python Array/List to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder     .appName(appName)     .master(master)     .getOrCreate()


# In[4]:


# List
data = [('Category A', None, "This is category A"),
        ('Category B', 120, "This is category B"),
        (None, 150, "This is category C")]

print(type(data))

# Create a schema for the dataframe
schema = StructType([
    StructField('Category', StringType(), True),
    StructField('Count', IntegerType(), True),
    StructField('Description', StringType(), True)
])

# Convert list to RDD
rdd = spark.sparkContext.parallelize(data)
print(type(rdd))
print(type(rdd.collect()))


# In[5]:


# Create data frame
df = spark.createDataFrame(rdd,schema)
print(df.schema)
df.show()


# In[7]:


updated = df.dropna(subset = "Count")
updated.show()


# In[9]:


header = updated.first()
header


# In[ ]:




