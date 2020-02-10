// Databricks notebook source
//val var
val variable1: String="hello raveena"

// COMMAND ----------

var variable_var : String = "hello raveena chiluka"
variable_var


// COMMAND ----------

var variable1 = "raveena"
var variable1=variable1+"chiluka"

// COMMAND ----------

var var1 = "raveena"
var1 = "raveena"+"chiluka"

// COMMAND ----------

val var1 = "raveena"
var1 = "raveena"+"chiluka"

// COMMAND ----------

var variable1 = "raveena"
var variable1 = variable1 +"chiluka"

// COMMAND ----------

val var_int : Int = 10;
val var_byte : Byte = 126;

// COMMAND ----------

print(f"hello, $var_int")

// COMMAND ----------

//switch statement---- match in scala
 var n: Int=213
n match{
  case 1 => print("1")
  case 2 => print("2")
  case 3 => print("found the value 3")
  case _ => print("not found")
}

// COMMAND ----------

var a: Int = 50
if(a==50){
  println("yes")
}
else{
  print("no")
}

// COMMAND ----------

for(i <- 1 until 5){
  println(i)
}

// COMMAND ----------

/* function
def <name>(argument : Type): Return_datatype={
commands
}*/
def add(a:Int,b : Int): Int={
  a+b
}
add(10,15)

// COMMAND ----------

def numberDivisor(a:Int,b:Int): Float={
  a/b
}
numberDivisor(35,7)

// COMMAND ----------

def table(a:Int): Unit={
  for(i <- 1 until 11){
    val c:Int = a*i
    println(f"$a X $i = $c")
  }
}
table(5)

// COMMAND ----------

//here func is a parameter to the third function with 2 int arguments with int return type , third is a function with y,func as arguments with int return type  
def third(y: Int,func : (Int, Int) => Int): Int={
  func(y,10)
}
third(4,add)

// COMMAND ----------

def double(x: Double,y:Double): Double={
  x/y
}
def addNumber(variable1 : Double, variable2: Double, func : (Double,Double) => Double): Double={
  func(variable1,variable2)
}
addNumber(35.0,7.0,double)

// COMMAND ----------

//val is immuatable
val tup = ("hello","chiluka",55)
tup._2
//here index value starts from one

// COMMAND ----------

//var is mutable
var dict= 1->"raveena"
dict._2

// COMMAND ----------

//to concatinate we use ++ symbol
var tup = ("hello","chiluka")
println(tup._1++tup._2)

// COMMAND ----------

var list1 = List(1,2,3)
var list2 = List(4,5,6)
println(list1++list2)

// COMMAND ----------

var newlist = List(1,2,3,4,5)
newlist.filter((x:Int)=>x!=3)
//creates a new list with filterd data

// COMMAND ----------

//if we have diff data types in list then we can use "any" as datatype, but here we have list of strings 
var liststring = List("hey","hello")
liststring.map((x:String)=>x.reverse)
liststring.map((x:String)=>x.length)

// COMMAND ----------

var list3 = List(1,2,3,4,5)
list3.reduce((x:Int,y:Int)=>x+y)
//first element is stored in x and next in y,then temp sum is stored in x and next element will be y and so on until all the elements are added and list will be reduced to single element

// COMMAND ----------

//here index is starting from zero
list3(3)

// COMMAND ----------

var list4=List("hey","raveena","class","is","boring")
list4.map((x:String)=>(x,1))

// COMMAND ----------

//var list4=List("hey","raveena","class","is","boring")
list4(0).map((x:String)=>(x,1))
//here map function is not applicable to single value, it is applicable only for list or tuple

// COMMAND ----------

var ss : Byte = 124
//ss.toString
ss.toInt
// for typecasting we have functions like toInt, toString, toDouble, toFloat etc...
