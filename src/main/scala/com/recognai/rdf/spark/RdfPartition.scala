package com.recognai.rdf.spark

import org.apache.spark.Partition


/**
  * Created by @frascuchon on 07/10/16.
  */
case class RdfPartition(rddId: Int, override val index: Int, subjectsURIs: Set[String])
  extends Partition
