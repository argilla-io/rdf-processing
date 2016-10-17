package com.recognai.rdf.spark.enrichment

import com.recognai.rdf.spark.operations.{LiteralProperty, ObjectProperty, Triple}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}


object module {

  private[enrichment] def parserProperty(value: String): ObjectProperty = {
    if (StringUtils.startsWith(value, "http")) ObjectProperty(value)
    else ObjectProperty(LiteralProperty(value, "string"))
  }

  private[enrichment] def getURIName(uri: String): String = uri.substring(uri.lastIndexOf("/") + 1)

  object udfFunctions {

    /**
      * Given a URI String, extract the URI name
      */
    @deprecated
    val uriName = udf[String, String](getURIName)
  }

}

/**
  * Created by @frascuchon on 16/10/2016.
  */
class WikidataEnrichment(wikidataPath: String) extends RDFEnrichment {

  import module._

  override def enrichRDF(input: Dataset[Triple])(implicit sparkSession: SparkSession): Dataset[Triple] = {

    val wikidata = readWikidataTriples(sparkSession, wikidataPath)
      .cache()
      .as("WIKIDATA")

    val matchedSubjects: Dataset[(String, String)] = findInputSubjectInWikidata(input, wikidata)
    createWikidataTriples(wikidata, matchedSubjects)
  }

  def readWikidataTriples(sparkSession: SparkSession, path: String)(implicit session:SparkSession): Dataset[Triple] = {
    import session.implicits._

    sparkSession.sqlContext.read
      .options(Map(
        "header" -> "true",
        "sep" -> ","
      ))
      .csv(path)
      .coalesce(16)
      .map(r => Triple(r.getString(0), r.getString(1), parserProperty(r.getString(2))))
  }

  /**
    *
    * @param wikidata        the wikidata dataset
    * @param matchedSubjects the wikidata-input subject relations dataset
    * @param sparkSession    the current spark session
    * @return [[Triple]] dataset with wikidata triples, replacing wikidata subject with input subject related in
    *         matched dataset
    */
  def createWikidataTriples(wikidata: Dataset[Triple], matchedSubjects: Dataset[(String, String)])
                           (implicit sparkSession: SparkSession): Dataset[Triple] = {
    import sparkSession.implicits._

    wikidata.joinWith(matchedSubjects, $"WIKIDATA.Subject" === $"MATCHED._1")
      .map { case (triple, subjectsRelation) => Triple(subjectsRelation._2, triple.Predicate, triple.Object) }
  }

  /**
    * Find relations between input subjects and wikidata subjects. Lookup wikidata triples with object defined as
    * subjects in input dataset
    *
    * @param input        the input dataset
    * @param wikidata     the wikidata dataset
    * @param sparkSession the current spark session
    * @return a  [[Dataset]] named "MATCHED", with tuples describing relation between wikidata subjects (_1) and input
    *         subjects (_2)
    */
  private def findInputSubjectInWikidata(input: Dataset[Triple], wikidata: Dataset[Triple])
                                        (implicit sparkSession: SparkSession): Dataset[(String, String)] = {

    import sparkSession.implicits._

    val dataset = wikidata.filter(_.Object.literal.isDefined).as("WD")

    input.map(t => (t.Subject, getURIName(t.Subject))).as("RDF")
      .joinWith(dataset, $"RDF._2" === $"WD.Object.literal.value")
      .map { case (t1, t2) => (t2.Subject, t1._1) }
      .distinct() as "MATCHED"
  }
}
