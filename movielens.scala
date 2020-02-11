// Databricks notebook source
import org.apache.spark._

// COMMAND ----------

val rdd = sc.textFile("/FileStore/tables/u.data")
rdd.collect()

// COMMAND ----------

//to list all the rating values i.e from 2nd column
val rating= rdd.map(x=>x.split("\t")(2))
rating.collect()

// COMMAND ----------

val keyRating=rating.map(x=>(x,1))

//keyRating.reduceByKey(_+_).collect()

// COMMAND ----------

val output = keyRating.countByValue()
output.collect()
