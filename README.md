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

![Mapreduce vs Spark](https://k21academy.com/wp-content/uploads/2018/10/Capture-1.png)
![Spark Architecture](https://i0.wp.com/0x0fff.com/wp-content/uploads/2015/03/Spark-Architecture-Official.png)
![Spark Component](https://cdn.analyticsvidhya.com/wp-content/uploads/2020/11/spark_Architecture.png)
