package com.recognai.rdf.spark

import com.recognai.rdf.spark.enrichment.WikidataEnrichment
import org.apache.spark.sql.SparkSession

/**
  * Created by @frascuchon on 07/10/16.
  */
object RDFIngestion extends App {

  private val path = "src/test/resources/autoridades.nt"
  private val wikipath = "src/test/resources/wd-bne.csv"

  implicit val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("RDF processor")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val wikidataEnrichment = new WikidataEnrichment(wikipath)

  val input = RDFRead(path)(sc).cache()

  val processed = RDFProcessing(input, Seq(wikidataEnrichment))

  processed.show()

}


