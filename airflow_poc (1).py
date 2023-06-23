#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder \
      .master("local[1]") \
      .appName("raja.deepak") \
      .getOrCreate() 


# In[4]:


df= spark.read.csv(r"C:\Users\deepa\Downloads\timberland_stock.csv", header = True)
df.show(10)


# In[5]:


from pyspark.sql.functions import year, month, dayofmonth


df = df.withColumn("year", year(df.Date))
df = df.withColumn("month", month(df.Date))
df = df.withColumn("day", dayofmonth(df.Date))


df.show(10)


# In[6]:


df.createOrReplaceTempView("timber")


# In[7]:


df2 = spark.sql("select Date,High from timber where High=(select max(High) from timber)")


# In[8]:


df2.show()


# In[9]:


mean_close = spark.sql("select mean(Close) as Mean_Close from timber")
mean_close.show()


# In[10]:


min_volume = spark.sql("select min(Volume) as Min_Volume from timber")
min_volume.show()


# In[11]:


max_volume = spark.sql("select max(Volume) as Max_Volume from timber")
max_volume.show()


# In[12]:


close60dollars = spark.sql("select Date as Close_ls_60 from timber where Close<60.00")
close60dollars.createOrReplaceTempView("close60dollars")
count60close = spark.sql("select count(*) as No_of_days_is60 from close60dollars")
count60close.show()


# In[13]:


pearson_corr = spark.sql("SELECT corr(High, Volume) AS pearson_corr from timber")
pearson_corr.show()


# In[14]:


max_high_per_year = spark.sql("select year, max(High) as max_high from timber group by year")
max_high_per_year.show()


# In[15]:


avg_close_per_month = spark.sql("select month, avg(Close) as avg_close_monthly from timber group by month order by month asc")
avg_close_per_month.show()


# In[ ]:




