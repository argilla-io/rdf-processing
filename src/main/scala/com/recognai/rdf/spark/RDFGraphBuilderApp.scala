package com.recognai.rdf.spark

import com.recognai.rdf.spark.enrichment.WikidataEnrichment
import com.recognai.rdf.spark.store.ESGraphStorage
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

/**
  * Created by @frascuchon on 07/10/16.
  */
object RDFGraphBuilderApp extends App {

  private val path = "rdf-processing/data/autoridades.nt"
  private val wikipath = "rdf-processing/data/wd-bne.csv"

  implicit val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("RDF processor")
    .getOrCreate()

  import sparkSession.implicits._

  val sc = sparkSession.sparkContext

  val wikidataEnrichment = new WikidataEnrichment(wikipath)

  val input = RDFRead(path)(sc).cache()

  val (subjects, triples) = RDFProcessing(input, Seq(wikidataEnrichment))

  val subjectsDF = subjects.toDF("id", "properties")
  val triplesDF = triples
    .flatMap(t => t.Object.uri.map(uri => (t.Subject, uri, t.Predicate)))
    .toDF("src", "dst", "entity")

  subjectsDF.printSchema()
  triplesDF.printSchema()

  val rdfGraph = GraphFrame(subjectsDF, triplesDF)

  //val results = rdfGraph.pageRank.resetProbability(0.01).maxIter(1).run()
  //results.vertices.show()

  ESGraphStorage(rdfGraph, "rdf/resources")
}


