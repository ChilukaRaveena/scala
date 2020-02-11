// Databricks notebook source
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// COMMAND ----------

//spc is sparkconf object and sc is sparkcontext object, setappname is used to provide a name to cluster
//val spc = new SparkConf().setMaster("localhost[*]").setAppName("First RDD")
//val sc = new SparkContext(spc)

// COMMAND ----------

//first way to create  RDD, using parallelize method
var list1= List(1,2,3,4,5)
val rdd = sc.parallelize(list1)
// do not use collect option when your data is larger than ram
rdd.collect()

// COMMAND ----------

var rdd1 = sc.parallelize(List(1,2,3)).collect()

// COMMAND ----------

rdd1.partitions.size

// COMMAND ----------

// if you want to provide number of partitions you want, then use below code i.e here no of partitions is 3
var rdd2 = sc.parallelize(List(1,2,3,4),3)
rdd2.partitions.size

// COMMAND ----------

// here tech is nothing but a variable
rdd2.map(tech => (tech * tech)).collect()

// COMMAND ----------

//takes the data only upto the particular partition
rdd2.map(tech => (tech * tech)).take(2)

// COMMAND ----------

//
rdd2.map(tech => (tech * tech)).top(2)

// COMMAND ----------

//flatmap takes 
val rdd3 = sc.parallelize(List("hey","hello"))
rdd3.flatMap(tech => tech+"!!!").collect()

// COMMAND ----------

//map takes whole string as single entity and perform operations
rdd3.map(tech => tech+"!!!").collect()

// COMMAND ----------

rdd3.filter(tech => tech=="hey").collect()

// COMMAND ----------

val rdd4 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
rdd4.filter(x =>x%2==0).collect()

// COMMAND ----------

// here we dont use collection method because the result we get is not a collection
rdd4.reduce( (x,y) => x+y)

// COMMAND ----------

rdd4.count

// COMMAND ----------

val rdd5 = sc.parallelize(List("hi","hello","hw r u"))
rdd5.map(x=>x.split(",")).collect()

// COMMAND ----------

rdd5.flatMap(x=>x.split(",")).collect()

// COMMAND ----------

val rdd6 = sc.parallelize(Seq((1,"raveena",123),(2,"chiluka",321)))
val rdd7 = sc.parallelize(Seq((3,"raveena",123),(2,"chiluka",321)))
/*in this union duplicate values are printed as they(how many times) are present i.e same like union all in sql
i.e, in normal unions duplicates are printed only once and
union all will print whole data from both inputs(lists). */
val out=rdd6.union(rdd7)
out.collect()

// COMMAND ----------

//to print only distinct values we use:
out.distinct().collect()

// COMMAND ----------

//only common data will be generated
rdd6.intersection(rdd7).collect()

// COMMAND ----------

// grouping data acc to keys
var rdd12 = sc.parallelize(Array(("one",1),("two",2),("three",3),("two",342)))
rdd12.groupByKey().collect()

// COMMAND ----------

//applying same value to every key 
val rdd13 = sc.parallelize(Array("one","two","three","one","five","two"))
rdd13.map(x=>(x,1)).collect()

// COMMAND ----------

//it reduces the list and gives number of times they are repeated
// reducebykey vs groupbykey ====imp
// reducebykey is preferred
rdd13.map(x=>(x,1)).reduceByKey(_+_).collect()

// COMMAND ----------


