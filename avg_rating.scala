// Databricks notebook source
val broad= sc.broadcast((Array(1,2,3,4,5)))
broad.value

// COMMAND ----------

val accum = sc.accumulator(0)

// COMMAND ----------

sc.parallelize(List(1,2,3,4,5)).foreach(x => accum+=x)
accum

// COMMAND ----------

import org.apache.spark._

// COMMAND ----------

val rdd = sc.textFile("/FileStore/tables/u.data")

// COMMAND ----------

def movie(rdd:String)={
  val fields = rdd.split("\t")
  
  val id = fields(0).toInt
 
  val rating = fields(2).toInt
  
 
  (id,rating)
}

// COMMAND ----------

rdd.map(movie).collect()

// COMMAND ----------

//even movie id's
val evenid= rdd.map(movie).filter(x=>(x._1%2==0))
evenid.collect()

// COMMAND ----------

//odd movie id's
val noteven = rdd.map(movie).filter(x=>(x._1%2 != 0))
noteven.collect()

// COMMAND ----------

// average rating of each even movie id 
val new1 = evenid.mapValues(x=>(x,1))
new1.collect()

// COMMAND ----------

val sum = new1.reduceByKey((x,y)=>(x._1+y._1 , x._2+y._2))
sum.collect()

// COMMAND ----------

val even_avg = sum.mapValues(x=>(x._1/x._2))
even_avg.collect()

// COMMAND ----------

val new2 = noteven.mapValues(x=>(x,1))
new2.collect()

// COMMAND ----------

val sum2= new2.reduceByKey((x,y) => (x._1+y._1 , x._2+y._2))
sum2.collect()

// COMMAND ----------

val odd_avg = sum2.mapValues(x=>(x._1/x._2))
odd_avg.collect()

// COMMAND ----------

val Outputrdd = even_avg.union(odd_avg)
//Outputrdd.collect()
Outputrdd.saveAsTextFile("/FileStore/tables/avg_data")

// COMMAND ----------

even_avg.intersection(odd_avg).collect()
