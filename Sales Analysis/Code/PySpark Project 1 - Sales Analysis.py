# Databricks notebook source
# MAGIC %md
# MAGIC File Locations
# MAGIC
# MAGIC /FileStore/tables/sales_csv.txt
# MAGIC
# MAGIC /FileStore/tables/menu_csv.txt

# COMMAND ----------

# DBTITLE 1,Sales Dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
  StructField("product_id", IntegerType(), True),
  StructField("customer_id", StringType(), True),
  StructField("order_date", DateType(), True),
  StructField("location", StringType(), True),
  StructField("source_order", StringType(), True)

])

sales_df = spark.read.format("csv").option("inferschema",'true').schema(schema).load("/FileStore/tables/sales_csv.txt")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Deriving Y,M,Q
from pyspark.sql.functions import month, year, quarter

sales_df = sales_df.withColumn("order_year", year(sales_df.order_date))
sales_df = sales_df.withColumn("order_month", month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter", quarter(sales_df.order_date))

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Menu Dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
  StructField("product_id", IntegerType(), True),
  StructField("product_name", StringType(), True),
  StructField("price", StringType(), True),

])

menu_df = spark.read.format("csv").option("inferschema",'true').schema(schema).load("/FileStore/tables/menu_csv.txt")
display(menu_df)

# COMMAND ----------

# MAGIC %md
# MAGIC KPI

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by each customer
total_amount_spent = (sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))

display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by Food Category
total_amount_spent_food_category = (sales_df.join(menu_df,'product_id').groupBy('product_name').agg({'price':'sum'}).orderBy('product_name'))

display(total_amount_spent_food_category)

# COMMAND ----------

# DBTITLE 1,Sales each Month
sales_each_month = (sales_df.join(menu_df,'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))

display(sales_each_month)

# COMMAND ----------

# DBTITLE 1,Yearly Sales
yearly_sales = (sales_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))

display(yearly_sales)

# COMMAND ----------

# DBTITLE 1,Quarterly Sales
quarterly_sales = (sales_df.join(menu_df,'product_id').groupBy('order_quarter').agg({'price':'sum'}).orderBy('order_quarter'))

display(quarterly_sales)

# COMMAND ----------

# DBTITLE 1,Count of product purchase
from pyspark.sql.functions import count

prod_count = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending=0)).drop('product_id')

display(prod_count)

# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
top_5 = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending=0)).drop('product_id').limit(5)

display(top_5)

# COMMAND ----------

# DBTITLE 1,Frequency of customer visited Restaurant
from pyspark.sql.functions import countDistinct

Count_restaurant = (sales_df.filter(sales_df.source_order == 'Restaurant').groupBy('customer_id').agg(countDistinct('order_date')))

display(Count_restaurant)

# COMMAND ----------

# DBTITLE 1,Sales by Country
country_sales = (sales_df.join(menu_df,'product_id').groupBy('location').agg({'price':'sum'}).orderBy('location'))

display(country_sales)

# COMMAND ----------

# DBTITLE 1,Sales by Source
source_sales = (sales_df.join(menu_df,'product_id').groupBy('source_order').agg({'price':'sum'}).orderBy('source_order'))

display(source_sales)
