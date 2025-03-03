# Databricks notebook source
# MAGIC %md
# MAGIC Data Loading

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Data Frame
df = spark.read.load("/FileStore/tables/googleplaystore.csv", format = 'csv', header = 'true', sep = ',', escape = '"', inferschema = 'true')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Data Cleaning

# COMMAND ----------

df.describe()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning
df = df.drop('size','Content Rating','Last Updated','Android Ver','Current Ver')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn('Reviews', col("Reviews").cast(IntegerType()))\
    .withColumn("Installs", regexp_replace(col("Installs"),"[^0-9]",""))\
    .withColumn("Installs", col("Installs").cast(IntegerType()))\
        .withColumn("Price", regexp_replace(col("Price"),"[$]",""))\
            .withColumn("Price", col("Price").cast(IntegerType()))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Switching to SQL

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView("apps")

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC select * from apps

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis

# COMMAND ----------

# DBTITLE 1,Top 10 reviews given
# MAGIC %sql
# MAGIC select App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit(10)

# COMMAND ----------

# DBTITLE 1,Top 10 Installed Apps
# MAGIC %sql
# MAGIC select App, Type, sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc limit(10)

# COMMAND ----------

# DBTITLE 1,Category wise Distribution
# MAGIC %sql
# MAGIC select Category, sum(Installs) from Apps
# MAGIC group by 1
# MAGIC order by 2 desc limit(10)

# COMMAND ----------

# DBTITLE 1,Top Paid Apps
# MAGIC %sql
# MAGIC select App, sum(Price) from apps
# MAGIC where Type = 'Paid'
# MAGIC group by 1
# MAGIC order by 2 desc limit(10)

# COMMAND ----------


