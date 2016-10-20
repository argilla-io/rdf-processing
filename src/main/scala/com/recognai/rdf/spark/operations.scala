package com.recognai.rdf.spark

import scala.language.postfixOps

/**
  * Created by @frascuchon on 16/10/2016.
  */
object operations {

  val TriplePattern = "^<([^>]+)>\\s+<([^>]+)>\\s+(<?[^>]+>?)" r

  val SUBJECT_TYPE_PREDICATE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

  private val URIPattern = "<[^>]+>".r

  sealed trait SubjectProperty {
    def isLiteral(): Boolean

    def value(): String
  }

  case class urisubjectproperty(override val value: String) extends SubjectProperty {
    override def isLiteral(): Boolean = false
  }

  case class LiteralProperty(override val value: String, datatype: String, lang: Option[String] = None) extends
    SubjectProperty {

    override def isLiteral(): Boolean = true
  }

  case class Triple(Subject: String, Predicate: String, Object: ObjectProperty)


  case class ObjectProperty(uri: Option[String] = None, literal: Option[LiteralProperty] = None) {

    def isLiteral(): Boolean = literal match {
      case Some(_) => true
      case _ => false
    }

    def isURI(): Boolean = uri match {
      case Some(_) => true
      case _ => false
    }
  }

  object ObjectProperty {

    def apply(uri: String): ObjectProperty = ObjectProperty(Some(uri), None)

    def apply(literal: LiteralProperty): ObjectProperty = ObjectProperty(None, Some(literal))
  }

  case class Subject(Subject: String, properties: Map[String, Seq[ObjectProperty]]) {

    def literals(): Map[String, Seq[LiteralProperty]] = ???

    // TODO: several types per subject
    def getType(): Option[String] = properties.get(SUBJECT_TYPE_PREDICATE).flatMap {
      case a :: _ => a.uri
      case _ => None
    }


  }

  def isURIProperty(o: String): Boolean = URIPattern.findFirstMatchIn(o) isDefined

  def getURIName(uri: String): String = uri.substring(uri.lastIndexOf("/") + 1)

}
