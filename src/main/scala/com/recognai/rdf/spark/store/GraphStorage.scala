package com.recognai.rdf.spark.store

import org.graphframes.GraphFrame

/**
  * Created by @frascuchon on 19/10/2016.
  */
trait GraphStorage {

  def store(graph: GraphFrame): Unit

}
