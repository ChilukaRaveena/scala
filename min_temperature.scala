// Databricks notebook source
import org.apache.spark._
import scala.math.max
import scala.math.min

// COMMAND ----------

def minTemperature(data:String)={
  val fields = data.split(",")
  //this is to get station id
  val station = fields(0)
  //to get temperature type
  val tempType = fields(2)
  //to get value of temperature
  val tempValue = fields(3).toFloat
  (station,tempType,tempValue)
}

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/1800.csv")

// COMMAND ----------

data.map(minTemperature).collect()

// COMMAND ----------

val newdata =data.map(minTemperature)
newdata.take(5)

// COMMAND ----------

val filtertemp = newdata.filter(x=>(x._2=="TMAX"))
//filtertemp.collect()
//val fd=filtertemp.filter((x:String)=>x!="TMAX")
//fd.collect()
val fd=filtertemp.map(x=>(x._1,x._3))
fd.collect()

// COMMAND ----------

fd.max
//here max works like, it adds all the temp values of same station(ex:ite00100554..) and sum of each stations are compared and greater sum will be executed

// COMMAND ----------

fd.reduceByKey((x,y)=> min(x,y)).take(2)

// COMMAND ----------

fd.reduceByKey((x,y)=> max(x,y)).take(2)
