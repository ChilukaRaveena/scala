# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("first pyspark").getOrCreate()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/emp.csv",inferSchema = True, header = True)
df.show()

# COMMAND ----------

#to fill all the null values
df.fillna("raveena").show()
# here we are providing string as input, so all the columns with string datatypes are filled with input(raveena)

# COMMAND ----------

df.show()

# COMMAND ----------

df.fillna(11).show()

# COMMAND ----------

df.fillna("raveena",subset="name").show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2= df.describe().take()[0][2]
df2

# COMMAND ----------

out = int(df2)

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType

# COMMAND ----------

df=df.select(df["sales"].cast(DoubleType()))

# COMMAND ----------



# COMMAND ----------

df.fillna(df2, subset='sales').show()
