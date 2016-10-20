package com.recognai.rdf.spark.store

import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.graphframes.GraphFrame


object ESGraphStorage {
  def apply(graph: GraphFrame, path: String): Unit = new ESGraphStorage(
    Map(
      ES_RESOURCE -> path
    )).store(graph)
}

/**
  * Created by @frascuchon on 19/10/2016.
  */
class ESGraphStorage(configuration: Map[String, String]) extends GraphStorage {

  override def store(graph: GraphFrame): Unit = {

    import org.elasticsearch.spark.sql._

    val vertices = graph.vertices
    vertices.saveToEs(configuration)
  }
}
