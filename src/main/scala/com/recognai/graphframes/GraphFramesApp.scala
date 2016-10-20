package com.recognai.graphframes

import com.recognai.rdf.spark.store.ESGraphStorage
import org.apache.spark.sql.SparkSession

/**
  * Created by @frascuchon on 10/10/2016.
  */
object GraphFramesApp extends App {


  val sparkSession = SparkSession
    .builder()
    .appName("Spark graphframe test application")
    .master("local[*]")
    .getOrCreate()
  sparkSession

  val v1 = sparkSession.sqlContext.createDataFrame(List(
    ("a", "Alice", 34, List(1,2,3,4)),
    ("b", "Bob", 36, List(2,3,4,5)),
    ("c", "Charlie", 30, List(34,6))
  )).toDF("id","name","age", "pene")

  val v2 = sparkSession.sqlContext.createDataFrame(List(
    ("d", "John", 19)
  )).toDF("id", "name", "age")

  val e = sparkSession.sqlContext.createDataFrame(List(
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("d", "a", "friend")
  )).toDF("src","dst","relationship")

  import org.graphframes.GraphFrame

  val g = GraphFrame(v1, e)

  v1.show()

  g.inDegrees.show()
  println(g.edges.filter("relationship = 'follow'").count())

  val results= g.pageRank.resetProbability(0.01).maxIter(2).run()
  //results.vertices.select("id","pagerank").show()

  ESGraphStorage(results, "graph/test")

}
