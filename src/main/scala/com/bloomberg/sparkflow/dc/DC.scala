package com.bloomberg.sparkflow.dc

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.language.implicitConversions

import scala.reflect.ClassTag
import com.bloomberg.sparkflow._

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
    if(!assigned){
      if (checkpointed){
        loadCheckpoint[T](getSignature, sc) match {
          case Some(existingRdd) => this.rdd = existingRdd
          case None =>
            assignSparkResults(sc)
            rdd.cache()
            saveCheckpoint(getSignature, rdd)
        }
      } else {
        assignSparkResults(sc)
      }
    }
    rdd
  }

  private def assignSparkResults(sc: SparkContext) = {
    synchronized {
      assert(!assigned)
      val (resultRdd, resultSchema) = computeSparkResults(sc)
      this.rdd = resultRdd
      this.schema = resultSchema
      assigned = true
    }
  }

  def getSchema(sc: SparkContext): Option[StructType] = {
    if(!assigned) {
      assignSparkResults(sc)
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
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.zipWithUniqueId, "zipWithUniqueId")
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

}

object DC {

  implicit def dcToPairDCFunctions[K, V](dc: DC[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def dcToDFFunctions(dc: DC[Row]): DataFrameDCFunctions = {
    new DataFrameDCFunctions(dc)
  }
}
