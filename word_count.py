#!/usr/bin/env python
# coding: utf-8

# In[3]:


import findspark
findspark.init()


# In[4]:


import pyspark
import random


# In[6]:


sc = pyspark.SparkContext.getOrCreate()
words = sc.textFile("/home/cgt_jpr_lin_nikhil/nikhil/spark/data/words.txt")
print(words)
print(words.getNumPartitions())
mapped_word = words.flatMap(lambda line: line.split(" "))
print(mapped_word.collect())


# In[7]:


ind = mapped_word.map(lambda word: (word,1))
print(ind.collect())


# In[8]:


result = ind.reduceByKey(lambda a,b: a+b)
print(result.collect())


# In[42]:


result.saveAsTextFile("/home/cgt_jpr_lin_nikhil/nikhil/spark/data/result.txt")


# In[ ]:




