package com.recognai.rdf.spark


import com.recognai.rdf.spark.operations._
import org.apache.jena.rdf.model.{Model, ModelFactory, RDFNode, Statement}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.util.Try


/**
  * Created by @frascuchon on 07/10/16.
  */
class JenaRDFRDD(sc: SparkContext, path: String, minPartitions: Int) extends RDD[Triple](sc, Seq.empty) {

  import scala.collection.JavaConverters._


  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Triple] = {

    def newTripleFrom(statement: Statement): Triple = {

      def fromObjectNode(getObject: RDFNode): ObjectProperty =
        if (getObject.isLiteral) {
          val literal = getObject.asLiteral()
          ObjectProperty(LiteralProperty(
            literal.getLexicalForm,
            Try(literal.getDatatype.getURI).getOrElse("xsd:string"),
            Some(literal.getLanguage)
          ))
        } else ObjectProperty(getObject.asResource().getURI)

      Triple(statement.getSubject.getURI, statement.getPredicate.getURI, fromObjectNode(statement.getObject))
    }

    val model = loadRDFModel()

    val triplets = split match {
      case RdfPartition(_, _, subjects) =>
        (for {
          subjectURI <- subjects
          statement <- model.listStatements(model.createResource(subjectURI), null, null) asScala

        } yield {
          statement.getObject.isLiteral
          newTripleFrom(statement)
        }) toIterator

      case _ => Iterator.empty
    }

    Try(model.close());
    triplets
  }

  def splitForPartitions(subjects: Set[String], subjectsPerPartition: Int): Seq[Set[String]] = {

    if (subjects.size <= subjectsPerPartition) Seq(subjects)
    else {
      val (subset, rest) = subjects.splitAt(subjectsPerPartition)
      Seq(subset) ++ splitForPartitions(rest, subjectsPerPartition)
    }
  }

  override protected def getPartitions: Array[Partition] = {

    val model = loadRDFModel()
    val nonBlankSubjects = model.listSubjects().asScala.filter(!_.isAnon).map(_.getURI) toSet

    val partitions = for {
      (subjects, partitionId) <- splitForPartitions(nonBlankSubjects, nonBlankSubjects.size / minPartitions).zipWithIndex
    } yield RdfPartition(id, partitionId, subjects)

    Try(model.close()); partitions.toArray
  }

  def loadRDFModel(): Model = {
    /*
    // Load HDT file using the hdt-java library
    val hdt = HDTManager.mapIndexedHDT(path, new ProgressListener() {
      override def notifyProgress(level: Float, message: String): Unit = logInfo(s"[$level]: $message")
    })

    // Create Jena Model on top of HDT.
    val graph = new HDTGraph(hdt)

    ModelFactory.createModelForGraph(graph)
    */
    val model = ModelFactory.createDefaultModel()

    RDFDataMgr.read(model, path, Lang.RDFXML) ; model
  }
}

