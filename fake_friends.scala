// Databricks notebook source
import org.apache.spark._
import scala.math.max
import scala.math.min
import scala.math.sum

// COMMAND ----------

val rdd1 = sc.textFile("/FileStore/tables/fakefriends.csv")

// COMMAND ----------

def fakeFriend(rdd1:String)={
  val fields = rdd1.split(",")
  //this is to get station id
  val age = fields(2).toInt
  //to get temperature type
  val noofFriends = fields(3).toInt
  //to get value of temperature
 
  (age,noofFriends)
}

// COMMAND ----------

rdd1.map(fakeFriend).collect()

// COMMAND ----------

val Friends=rdd1.map(fakeFriend)
Friends.reduceByKey((x,y)=> min(x,y)).collect()

// COMMAND ----------

Friends.reduceByKey((x,y)=> max(x,y)).collect()

// COMMAND ----------

val new1 = Friends.mapValues(x=>(x,1))
new1.collect()

// COMMAND ----------

val new2 = new1.reduceByKey((x,y)=>(x._1+y._1 , x._2+y._2))
new2.collect()

// COMMAND ----------

new2.mapValues(x=>(x._1/x._2)).collect()
