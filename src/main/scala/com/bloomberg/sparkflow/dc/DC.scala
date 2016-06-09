package com.bloomberg.sparkflow.dc

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.language.implicitConversions

import scala.reflect.ClassTag
import com.bloomberg.sparkflow
import com.bloomberg.sparkflow._
import scala.reflect.runtime.universe.TypeTag
import scala.reflect._
import sparkflow.dc.Util._

/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T: ClassTag](deps: Seq[Dependency[_]]) extends Dependency[T](deps) {

  private var rdd: RDD[T] = _
  private var checkpointed = false
  private var schema: Option[StructType] = None
  private var assigned = false

  protected def computeSparkResults(sc: SparkContext): (RDD[T], Option[StructType])


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

  def sample(
              withReplacement: Boolean,
              fraction: Double): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.sample(withReplacement, fraction), Seq("sample", withReplacement, fraction))
  }

  def sample(
              withReplacement: Boolean,
              fraction: Double,
              seed: Long): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.sample(withReplacement, fraction, seed), Seq("sample", withReplacement, fraction, seed))
  }

  def union(other: DC[T]): DC[T] = {
    new UnionDC[T](this, other)
  }

  def ++(other: DC[T]): DC[T] = {
    this.union(other)
  }

  def mapToResult[U:ClassTag](f: RDD[T] => U): DR[U] ={
    new DRImpl[T,U](this, f)
  }

  def withResult[U: ClassTag](dr: DR[U]): DC[(T,U)] = {
    new ResultDepDC(this, dr)
  }

  def checkpoint(): this.type = {
    this.checkpointed = true
    this
  }

  def distinct(): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.distinct(), Seq("distinct"))
  }

  def distinct(numPartitions: Int): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.distinct(numPartitions), Seq("distinct", numPartitions.toString))
  }

  def repartition(numPartitions: Int): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.repartition(numPartitions), Seq("repartition", numPartitions.toString))
  }

  def coalesce(numPartitions: Int, shuffle: Boolean = false): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.coalesce(numPartitions, shuffle), Seq("coalesce", numPartitions.toString, shuffle.toString))
  }

  def intersection(other: DC[T]): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.intersection(right)
    }
    new MultiInputDC(this, other, resultFunc)
  }

  def intersection(other: DC[T], numPartitions: Int): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.intersection(right, numPartitions)
    }
    new MultiInputDC(this, other, resultFunc)
  }

  def glom(): DC[Array[T]] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.glom(), Seq("glom"))
  }

  def cartesian[U: ClassTag](other: DC[U]): DC[(T, U)] = {
    val resultFunc = (left: RDD[T], right: RDD[U]) => {
      left.cartesian(right)
    }
    new MultiInputDC(this, other, resultFunc)
  }

  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): DC[(K, Iterable[T])] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.groupBy(f), f, Seq("groupBy"))
  }

  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): DC[(K, Iterable[T])] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.groupBy(f, numPartitions), f, Seq("groupBy", numPartitions.toString))
  }

  def zip[U: ClassTag](other: DC[U]): DC[(T, U)] = {
    val resultFunc = (left: RDD[T], right: RDD[U]) => {
      left.zip(right)
    }
    new MultiInputDC(this, other, resultFunc)
  }

  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean = true)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.sortBy(f, ascending), f, Seq("sortBy", ascending.toString))
  }

  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean,
                 numPartitions: Int)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.sortBy(f, ascending, numPartitions), Seq("sortBy", ascending.toString, numPartitions.toString))
  }

  def keyBy[K](f: T => K): DC[(K,T)] ={
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.keyBy(f), f, Seq("keyBy"))
  }

  def sliding(windowSize: Int): DC[Array[T]] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.sliding(windowSize), Seq("sliding", windowSize.toString))
  }

  def mapPartitions[U: ClassTag](
                                  f: Iterator[T] => Iterator[U],
                                  preservesPartitioning: Boolean = false): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.mapPartitions(f, preservesPartitioning), f, Seq(preservesPartitioning.toString))
  }

  def getRDD(sc: SparkContext): RDD[T] = {

    synchronized {
      if (!assigned) {
        if (checkpointed) {
          loadCheckpoint[T](checkpointPath, sc, dataFrameBacked) match {
            case Some((resultRdd, resultSchema)) => assignSparkResults(resultRdd, resultSchema)
            case None =>
              val (resultRDD, resultSchema) = computeSparkResults(sc)
              saveCheckpoint(checkpointPath, resultRDD, resultSchema, dataFrameBacked)
              loadCheckpoint[T](checkpointPath, sc, dataFrameBacked) match {
                case Some((rRDD, rSchema)) => assignSparkResults(rRDD, rSchema)
                case None => throw new RuntimeException(s"failed to persist to: $checkpointPath")
              }
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

  private def checkpointPath = new File(checkpointDir, getSignature).toString

  private def assignSparkResults(resultRdd: RDD[T], resultSchema: Option[StructType]) = {
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

  private def dataFrameBacked = {
    this.ct.equals(classTag[Row])
  }

}

object DC {

  implicit def dcToPairDCFunctions[K, V](dc: DC[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def dcToSecondaryPairDCFunctions[K, K2, V](dc: DC[((K,K2), V)])
    (implicit kt: ClassTag[K], k2t: ClassTag[K2],vt: ClassTag[V], ord: Ordering[(K,K2)] = null): SecondaryPairDCFunctions[K, K2, V] = {
    new SecondaryPairDCFunctions(dc)
  }

  implicit def dcToDFFunctions(dc: DC[Row]): DataFrameDCFunctions = {
    new DataFrameDCFunctions(dc)
  }

  implicit def dcToDoubleFunctions(dc: DC[Double]): DoubleDCFunctions = {
    new DoubleDCFunctions(dc)
  }

  implicit def dcToProductDCFunctions[T <: Product : TypeTag](dc: DC[T]): ProductDCFunctions[T] = {
    new ProductDCFunctions[T](dc)
  }
}
