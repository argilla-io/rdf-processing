package com.recognai.rdf.spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by @frascuchon on 16/10/2016.
  */
object RDFRead {

  import operations._

  private def cleanURI(theObject: String) = StringUtils.removeEnd(StringUtils.removeStart(theObject, "<"), ">")

  def parseProperty(o: String): ObjectProperty = {
    if (isURIProperty(o)) ObjectProperty(cleanURI(o))
    else ObjectProperty(LiteralProperty(o, "string"))
  }

  def apply(path: String)(sc: SparkContext): RDD[Triple] =
    sc.textFile(path)
      .flatMap(TriplePattern.findAllIn(_).flatMap {
        case TriplePattern(s, p, o) => Some(Triple(s, p, parseProperty(o)))
        case _ => None
      })

}
