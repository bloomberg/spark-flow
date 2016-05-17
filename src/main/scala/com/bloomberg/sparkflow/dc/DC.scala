package com.bloomberg.sparkflow.dc

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types.StructType
import scala.language.implicitConversions

import scala.reflect.ClassTag
import com.bloomberg.sparkflow
import com.bloomberg.sparkflow._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try


/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T: ClassTag](deps: Seq[Dependency[_]]) extends Dependency[T](deps) {

  private var rdd: RDD[T] = _
  private var checkpointed = false
  private var schema: Option[StructType] = None

  private var assigned = false

  protected def computeSparkResults(sc: SparkContext): (RDD[T], Option[StructType])

  def getRDD(sc: SparkContext): RDD[T] = {

    synchronized {
      if (!assigned) {
        if (checkpointed) {
          loadCheckpoint(sc) match {
            case Some((resultRdd, resultSchema)) => assignSparkResults(resultRdd, resultSchema)
            case None =>
              assignComputedSparkResults(sc)
              rdd.cache()
              saveCheckpoint()
          }
        } else {
          assignComputedSparkResults(sc)
        }
      }
    }
    rdd
  }

  def getSchema(sc: SparkContext): Option[StructType] = {
    if(!assigned) {
      getRDD(sc)
    }
    schema
  }

  def map[U: ClassTag](f: T => U): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.map(f), f)
  }

  def filter(f: T => Boolean): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.filter(f), f)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.flatMap(f), f)
  }

  def zipWithUniqueId(): DC[(T, Long)] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.zipWithUniqueId, Seq("zipWithUniqueId"))
  }

  def mapToResult[U:ClassTag](f: RDD[T] => U): DR[U] ={
    new DRImpl[T,U](this, f)
  }

  def mapWith[U:ClassTag, V:ClassTag](dr: DR[U])(f: (T,U) => V) = {
    new ResultDepDC(this, dr, f)
  }

  def checkpoint(): this.type = {
    this.checkpointed = true
    this
  }

  def repartition(numPartitions: Int) = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.repartition(numPartitions), Seq("repartition", numPartitions.toString))
  }

  private def saveCheckpoint() = {
    assert(rdd != null)
    if (schema.isDefined){
      val rowDC = this.asInstanceOf[DC[Row]]
      val df = rowDC.getDF(rdd.sparkContext)
      df.write.parquet(checkpointPath)
    } else {
      rdd.saveAsObjectFile(checkpointPath)
    }

  }

  private def loadCheckpoint(sc: SparkContext): Option[(RDD[T], Option[StructType])] = {
    val dfAttempt = attemptDFLoad(sc)
    dfAttempt match {
      case Some(df) => Some((df.rdd.asInstanceOf[RDD[T]], Some(df.schema)))
      case None =>
        val rddAttempt = attemptRDDLoad(sc)
        rddAttempt match {
          case Some(resultRDD) => Some((resultRDD, None))
          case None => None
        }
    }

  }

  private def attemptDFLoad(sc: SparkContext): Option[DataFrame] = {
    Try{
      val sqlContext = SQLContext.getOrCreate(sc)
      val attemptDF = sqlContext.read.parquet(checkpointPath)
      attemptDF.first()
      attemptDF
    }.toOption
  }

  private def attemptRDDLoad(sc: SparkContext): Option[RDD[T]] = {
    Try{
      val attemptRDD = sc.objectFile[T](new File(checkpointDir, getSignature).toString)
      attemptRDD.first()
      attemptRDD
    }.toOption
  }

  private def checkpointPath = new File(checkpointDir, getSignature).toString


  def assignSparkResults(resultRdd: RDD[T], resultSchema: Option[StructType]) = {
    synchronized {
      assert(!assigned)
      this.rdd = resultRdd
      this.schema = resultSchema
      assigned = true
    }
  }

  private def assignComputedSparkResults(sc: SparkContext) = {
    synchronized {
      assert(!assigned)
      val (resultRdd, resultSchema) = computeSparkResults(sc)
      assignSparkResults(resultRdd, resultSchema)
    }
  }

}

object DC {

  implicit def dcToPairDCFunctions[K, V](dc: DC[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def dcToDFFunctions(dc: DC[Row]): DataFrameDCFunctions = {
    new DataFrameDCFunctions(dc)
  }

  implicit def dcToProductDCFunctions[T <: Product : TypeTag](dc: DC[T]): ProductDCFunctions[T] = {
    new ProductDCFunctions[T](dc)
  }
}
