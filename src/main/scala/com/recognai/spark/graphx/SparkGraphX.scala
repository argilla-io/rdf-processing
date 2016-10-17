package com.recognai.spark.graphx


/**
  * Created by @frascuchon on 07/10/16.
  */
object SparkGraphX extends App {

  import org.apache.spark._
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  val conf = new SparkConf().setMaster("local[*]").setAppName("graphX-test")
  val sc = new SparkContext(conf)

  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

  val defaultUser = ("John Doe", "Missing")

  val graph = Graph(users, relationships, defaultUser)


  val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

  for (elem <- facts.collect) {
    println(elem)
  }

  private val inDegrees = graph.inDegrees

  inDegrees.foreach(println)
}
