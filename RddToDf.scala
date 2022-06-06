// Databricks notebook source
val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------

val df = rdd.toDF()
df.show()
