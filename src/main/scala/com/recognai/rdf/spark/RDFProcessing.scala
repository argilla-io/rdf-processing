package com.recognai.rdf.spark

import com.recognai.rdf.spark.enrichment.RDFEnrichment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by @frascuchon on 16/10/2016.
  */
object RDFProcessing {

  import operations._

  def apply(inputRDF: RDD[Triple])(implicit sparkSession: SparkSession): Dataset[Subject] = apply(inputRDF, Seq.empty)

  def apply(inputRDF: RDD[Triple], enrichments: Seq[RDFEnrichment])(implicit sparkSession: SparkSession)
  : Dataset[Subject] = {
    import sparkSession.implicits._

    val rdfDF = inputRDF.toDS().as("RDF")

    val enrichedRDF = enrichments.foldLeft(rdfDF)((rdf, enrichment) => rdf.union(enrichment.enrichRDF(rdf)))

    enrichedRDF
      .groupByKey(_.Subject)
      .mapGroups { (subject, groupData) => Subject(subject, createPropertiesMap(groupData)) }
  }

  private def findRDFTypes(rdf: Dataset[Triple])(implicit sparkSession: SparkSession): Dataset[TypeTuple] = {

    import sparkSession.implicits._

    rdf
      .filter(_.Predicate == SUBJECT_TYPE_PREDICATE)
      .map { case Triple(s, _, ObjectProperty(Some(uri), _)) => TypeTuple(s, uri) }
  }

  private def createPropertiesMap(group: Iterator[Triple])
  : Map[String, Seq[ObjectProperty]] = {

    def mergeProperties(map: Map[String, Seq[ObjectProperty]], triple: Triple)
    : Map[String, Seq[ObjectProperty]] = {

      val value = map.getOrElse(triple.Predicate, Seq.empty);
      map + (triple.Predicate -> value.+:(triple.Object))
    }

    group.foldLeft(Map.empty[String, Seq[ObjectProperty]])(mergeProperties)
  }
}
