package com.recognai.rdf.spark.enrichment

import com.recognai.rdf.spark.operations
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by @frascuchon on 16/10/2016.
  */
trait RDFEnrichment {

  import operations._

  def enrichRDF(input: Dataset[Triple])(implicit sparkSession: SparkSession): Dataset[Triple]

}
