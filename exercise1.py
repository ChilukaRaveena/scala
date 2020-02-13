# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("first pyspark").getOrCreate()

# COMMAND ----------

#creating data frame object
df = spark.read.csv("/FileStore/tables/fakefriends.csv")
df.show()
# here column names are also coming in the column values 

# COMMAND ----------

# to avoid above problem we use header=true
df = spark.read.csv("/FileStore/tables/fakefriends.csv", inferSchema = True, header = True )
df.show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.schema

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.head(5)

# COMMAND ----------

df

# COMMAND ----------

#to select a particular column
df.select("Name").show()

# COMMAND ----------

df.select("Name", "Age").show()

# COMMAND ----------

from pyspark.sql.functions import mean

# COMMAND ----------

df2 = df.select(mean("Age").alias("mean_age")).show()

# COMMAND ----------

#we shoudn't use show with group by beacause it usese agregate functiuons like max,min,count etc..
grp = df.groupBy("Age").show()

# COMMAND ----------

grp = df.groupBy("Age").max().show()

# COMMAND ----------

max_age = df.groupBy("Age").max()
max_age.select("Age","max(noOfFriends)").show()

# COMMAND ----------

max_age.show()

# COMMAND ----------

# to rename a particular column
max_age.withColumnRenamed("max(noOfFriends)","maxFriends").select("age","maxFriends").show()

# COMMAND ----------

#to add a particular column to data frame
df.withColumn("Age/NoOfFriends", df["NoOfFriends"]/df["Age"])

# COMMAND ----------

# existing df will be replaced by added columns
df = df.withColumn("Age/NoOfFriends", df["NoOfFriends"]/df["Age"])

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import format_number
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType

# COMMAND ----------

df2=df.select(format_number("Age/NoOfFriends",2).alias("AgeByFriends").cast(IntegerType()))
df2.describe().show()
#df2.orderBy(df2["AgeByString"].desc()).show()


# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2.select("AgeByFriends").show()

# COMMAND ----------

#to convert a cloumn name from string to integer
df2.select("AgeByFriends".cast(DoubleType())).show()
