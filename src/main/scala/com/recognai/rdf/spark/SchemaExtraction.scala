package com.recognai.rdf.spark

import com.recognai.rdf.spark.operations.Subject
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by @frascuchon on 19/10/2016.
  */
object SchemaExtraction {

  case class Type(name: Option[String], propertiesDefinition: Map[String, Boolean])

  /**
    * Build the schema from a subjects dataset
    *
    * @param subjects     the subject dataset
    * @param sparkSession the current spark session
    * @return A Dataset with inferred schema types
    */
  def apply(subjects: Dataset[Subject])(implicit sparkSession: SparkSession): Dataset[Type] = {

    import sparkSession.implicits._

    subjects.map(s => (s.getType(), propertiesDefinitions(s)))
      .groupByKey(_._1)
      .mapGroups { case (t, properties) => Type(t, properties.map(_._2).reduce(_ ++ _)) }
  }

  private def propertiesDefinitions(s: Subject): Map[String, Boolean] =
    s.properties.map { case (name, values) => (name, values.exists(_.isLiteral())) }

}
