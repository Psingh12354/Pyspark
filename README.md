# Pyspark
Apache Spark is written in Scala programming language. To support Python with Spark, Apache Spark community released a tool, PySpark. Using PySpark, you can work with RDDs in Python programming language also. It is because of a library called Py4j that they are able to achieve this.

### Some key points
- Pyspark is the combo of python spark
- Scalable
- 100x faster than hadoop mapreduce
- 10x faster is disk
- Use Ram insted of local drive which increase the processing

### Operation on RDD

- **Transformation-:** It create a new rdd ex: map, flatMap, filter, groupby etc.
-  **Actions-:** It is to perform ceratin computation like count, collect, take, first etc it doesn't create a new rdd

![Mapreduce vs Spark](https://data-flair.training/blogs/wp-content/uploads/sites/2/2016/09/Hadoop-MapReduce-vs-Apache-Spark.jpg)
![Spark Architecture](https://i0.wp.com/0x0fff.com/wp-content/uploads/2015/03/Spark-Architecture-Official.png)
![Spark Component](https://cdn.analyticsvidhya.com/wp-content/uploads/2020/11/spark_Architecture.png)

## Spark Components

### Spark Streaming
Spark Streaming is one of those unique features, which have empowered Spark to potentially take the role of Apache Storm. Spark Streaming mainly enables you to create analytical and interactive applications for live streaming data. You can do the streaming of the data and then, Spark can run its operations from the streamed data itself.
- real time object detection are the example

### MLLib
MLLib is a machine learning library like Mahout. It is built on top of Spark, and has the provision to support many machine learning algorithms. But the point difference with Mahout is that it runs almost 100 times faster than MapReduce. It is not yet as enriched as Mahout, but it is coming up pretty well, even though it is still in the initial stage of growth.

### GraphX
For graphs and graphical computations, Spark has its own Graph Computation Engine, called GraphX. It is similar to other widely used graph processing tools or databases, like Neo4j, Girafe, and many other distributed graph databases.

### Spark SQL
The Spark SQL is built on the top of Spark Core. It provides support for structured data.
