package com.bloomberg.sparkflow.dc

import java.io.File

import com.bloomberg.sparkflow
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.dc.Util._
import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.EncoderUtil.encoderFor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.collection.Map
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T](encoder: Encoder[T], deps: Seq[Dependency[_]]) extends Dependency[T](deps) {

  private var dataset: Dataset[T] = _
  private var checkpointed = false
  private var assigned = false

  protected def computeDataset(spark: SparkSession): Dataset[T]

  protected implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)

  protected implicit def classTag = exprEnc.clsTag

  def as[U : Encoder]: DC[U] = {
    val uEncoder = encoderFor[U]
    new DatasetTransformDC(uEncoder, this, (ds: Dataset[T]) => ds.as[U], Seq("as"))
  }


  def map[U: Encoder](f: T => U): DC[U] = {
    val uEncoder = encoderFor[U]
    new DatasetTransformDC(uEncoder, this, (ds: Dataset[T]) => ds.map(f), Seq(hashClass(f)))
  }

  def filter(f: T => Boolean): DC[T] = {
    new DatasetTransformDC(encoder, this, (ds: Dataset[T]) => ds.filter(f), Seq(hashClass(f)))
  }

  def flatMap[U: Encoder](f: T => TraversableOnce[U]): DC[U] = {
    val uEncoder = encoderFor[U]
    new DatasetTransformDC(uEncoder, this, (ds: Dataset[T]) => ds.flatMap(f), Seq(hashClass(f)))
  }

  def zipWithUniqueId(): DC[(T, Long)] = {
    val longEnc = encoderFor[Long]
    val enc = ExpressionEncoder.tuple(exprEnc, longEnc)
    new RDDTransformDC(enc, this, (rdd: RDD[T]) => rdd.zipWithUniqueId, Seq("zipWithUniqueId"))
  }

  def groupBy[K: Encoder : ClassTag](func: T => K)(implicit kvEncoder: Encoder[(K, T)]): KeyValueGroupedDC[K, T] = {
    new KeyValueGroupedDCImpl(this, (ds: Dataset[T]) => ds.groupByKey(func), Seq())
  }

  def sample(
              withReplacement: Boolean,
              fraction: Double): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.sample(withReplacement, fraction), Seq("sample", withReplacement.toString, fraction.toString))
  }

  def sample(
              withReplacement: Boolean,
              fraction: Double,
              seed: Long): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.sample(withReplacement, fraction, seed), Seq("sample", withReplacement.toString, fraction.toString, seed.toString))
  }

  def union(other: DC[T]): DC[T] = {
    new UnionDC[T](this, other)
  }

  def ++(other: DC[T]): DC[T] = {
    this.union(other)
  }

  def mapToResult[U](f: RDD[T] => U): DR[U] = {
    new DRImpl[T, U](this, f)
  }

  def withResult[U: Encoder](dr: DR[U]): DC[(T, U)] = {
    val enc = ExpressionEncoder.tuple(exprEnc, encoderFor[U])
    new ResultDepDC(enc, this, dr)
  }

  def checkpoint(): this.type = {
    this.checkpointed = true
    this
  }

  def distinct(): DC[T] = {
    new DatasetTransformDC(encoder, this, (ds: Dataset[T]) => ds.distinct(), Seq("distinct"))
  }

  def distinct(numPartitions: Int): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.distinct(numPartitions), Seq("distinct", numPartitions.toString))
  }

  def repartition(numPartitions: Int): DC[T] = {
    new DatasetTransformDC(encoder, this, (ds: Dataset[T]) => ds.repartition(numPartitions), Seq("repartition", numPartitions.toString))
  }

  def coalesce(numPartitions: Int, shuffle: Boolean = false): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.coalesce(numPartitions, shuffle), Seq("coalesce", numPartitions.toString, shuffle.toString))
  }

  def intersection(other: DC[T]): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.intersection(right)
    }
    new MultiInputDC(encoder, this, other, resultFunc)
  }

  def intersection(other: DC[T], numPartitions: Int): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.intersection(right, numPartitions)
    }
    new MultiInputDC(encoder, this, other, resultFunc)
  }

  def glom()(implicit aEncoder: Encoder[Array[T]]): DC[Array[T]] = {
    val arrEncoder = encoderFor[Array[T]]
    new RDDTransformDC(arrEncoder, this, (rdd: RDD[T]) => rdd.glom(), Seq("glom"))
  }

  def cartesian[U: Encoder](other: DC[U]): DC[(T, U)] = {
    val enc = ExpressionEncoder.tuple(exprEnc, encoderFor[U])
    implicit val uClassTag = encoderFor[U].clsTag
    val resultFunc = (left: RDD[T], right: RDD[U]) => {
      left.cartesian(right)
    }
    new MultiInputDC(enc, this, other, resultFunc)
  }

  //  def groupBy[K:Encoder](f: T => K): DC[(K, Array[T])] = {
  //    val arrayEnc = encFor[Array[T]]
  //    val enc = ExpressionEncoder.tuple(encFor[K], arrayEnc)
  //    new RDDTransformDC(enc, this, (rdd: RDD[T]) => rdd.groupBy(f).map{case (k, values) => (k, values.toArray)}, Seq("groupBy", hashClass(f)))
  //  }
  //
  //  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): DC[(K, Iterable[T])] = {
  //    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.groupBy(f, numPartitions), f, Seq("groupBy", numPartitions.toString))
  //  }

  def zip[U: Encoder](other: DC[U]): DC[(T, U)] = {
    val enc = ExpressionEncoder.tuple(exprEnc, encoderFor[U])
    implicit val uClassTag = encoderFor[U].clsTag
    val resultFunc = (left: RDD[T], right: RDD[U]) => {
      left.zip(right)
    }
    new MultiInputDC(enc, this, other, resultFunc)
  }

  def zipWithIndex: DC[(T, Long)] = {
    val enc = ExpressionEncoder.tuple(exprEnc, encoderFor[Long])
    new RDDTransformDC(enc, this, (rdd: RDD[T]) => rdd.zipWithIndex, Seq("zipWithIndex"))
  }

  def subtract(other: DC[T]): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.subtract(right)
    }
    new MultiInputDC(encoder, this, other, resultFunc)
  }

  def subtract(other: DC[T], numPartitions: Int): DC[T] = {
    val resultFunc = (left: RDD[T], right: RDD[T]) => {
      left.subtract(right, numPartitions)
    }
    new MultiInputDC(encoder, this, other, resultFunc)
  }

  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean = true)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.sortBy(f, ascending), Seq(hashClass(f), "sortBy", ascending.toString))
  }

  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean,
                 numPartitions: Int)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): DC[T] = {
    new RDDTransformDC(encoder, this, (rdd: RDD[T]) => rdd.sortBy(f, ascending, numPartitions), Seq(hashClass(f), "sortBy", ascending.toString, numPartitions.toString))
  }

  def keyBy[K: Encoder](f: T => K): DC[(K, T)] = {
    val enc = ExpressionEncoder.tuple(encoderFor[K], exprEnc)
    new RDDTransformDC(enc, this, (rdd: RDD[T]) => rdd.keyBy(f), Seq(hashClass(f), "keyBy"))
  }

  def sliding(windowSize: Int)(implicit aEncoder: Encoder[Array[T]]): DC[Array[T]] = {
    val arrEncoder = encoderFor[Array[T]]
    new RDDTransformDC(arrEncoder, this, (rdd: RDD[T]) => rdd.sliding(windowSize), Seq("sliding", windowSize.toString))
  }

  def mapPartitions[U: Encoder](
                                 f: Iterator[T] => Iterator[U],
                                 preservesPartitioning: Boolean = false)(implicit uEncoder: Encoder[U]): DC[U] = {
    val uEncoder = encoderFor[U]
    new DatasetTransformDC(uEncoder, this, (ds: Dataset[T]) => ds.mapPartitions(f), Seq(hashClass(f)))
  }


  /*
  Dataframe stuff
   */

  def toDF(): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.toDF()
    }
    new DatasetTransformDC(emptyRowEncoder, this, f, Seq("toDF"))
  }

  def toDF(colNames: String*): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.toDF(colNames: _*)
    }
    new DatasetTransformDC(emptyRowEncoder, this, f, colNames)
  }


    @scala.annotation.varargs
  def select(cols: Column*): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.select(cols: _*)
    }
    val hashTarget = cols.map(_.toString())
    new DatasetTransformDC(emptyRowEncoder, this, f, hashTarget)
  }

  @scala.annotation.varargs
  def select(col: String, cols: String*): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.select(col, cols: _*)
    }
    val hashTarget = Seq("select", col) ++ cols
    new DatasetTransformDC(emptyRowEncoder, this, f, hashTarget)
  }

  def selectExpr(exprs: String*): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.selectExpr(exprs: _*)
    }
    val hashTarget = Seq("selectExpr") ++ exprs
    new DatasetTransformDC(emptyRowEncoder, this, f, hashTarget)

  }

  def select[U1: Encoder](c1: TypedColumn[T, U1]): DC[U1] = {
    val u1Encoder = encoderFor[U1]
    val f = (ds: Dataset[T]) => {
      ds.select(c1)
    }
    val hashTarget = Seq("select") :+ c1.toString()
    new DatasetTransformDC(u1Encoder, this, f, hashTarget)
  }


  def filter(condition: Column): DC[T] = {
    val f = (ds: Dataset[T]) => {
      ds.filter(condition)
    }

    val hashTarget = Seq("filter", condition.toString())
    new DatasetTransformDC(encoder, this, f, hashTarget)
  }

  def drop(colName: String): DC[Row] = {
    val f = (ds: Dataset[T]) => {
      ds.drop(colName)
    }
    val hashTarget = Seq("drop", colName)
    new DatasetTransformDC(emptyRowEncoder, this, f, hashTarget)
  }

  def apply(colName: String) = {
    new Column(colName)
  }

//  def join(right: DC[Row]): DC[Row] = {
//    val f = (left: Dataset[_], right: Dataset[_]) => {
//      left.join(right)
//    }
//    val hashTarget = Seq("join")
//    new MultiDatasetTransformDC(this, right, f, hashTarget)
//  }

//  def join(right: DC[Row], usingColumn: String): DC[Row] = join(right, usingColumn)
//
//  def join(right: DC[Row], joinExprs: Column): DC[Row] = join(right, joinExprs, "inner")
//
//  def join(right: DC[Row], joinExprs: Column, joinType: String): DC[Row] = {
//    val f = (left: Dataset[_], right: Dataset[_]) => {
//      left.join(right, joinExprs, joinType)
//    }
//    val hashTarget = Seq("join", joinType, joinExprs.toString())
//    new MultiDatasetTransformDC(this, right, f, hashTarget)
//  }


  def getRDD(spark: SparkSession): RDD[T] = {
    getDataset(spark).rdd
  }

  def mapPartitionsWithIndex[U: Encoder](
                                          f: (Int, Iterator[T]) => Iterator[U],
                                          preservesPartitioning: Boolean = false): DC[U] = {
    val uEncoder = encoderFor[U]
    implicit val uClassTag = encoderFor[U].clsTag
    new RDDTransformDC(uEncoder, this, (rdd: RDD[T]) => rdd.mapPartitionsWithIndex(f, preservesPartitioning), Seq(hashClass(f), preservesPartitioning.toString))
  }

  def getRDD(sc: SparkContext): RDD[T] = {
    getDataset(getSpark(sc)).rdd
  }

  def getDF(sc: SparkContext): DataFrame = {
    getDF(getSpark(sc))
  }

  def getDF(spark: SparkSession): DataFrame = {
    val df = getDataset(spark).toDF()
    df.toDF()
  }

  def getDataset(sc: SparkContext): Dataset[T] = {
    getDataset(getSpark(sc))
  }

  def getDataset(spark: SparkSession): Dataset[T] = {
    synchronized {
      sparkflow.setSession(spark)
      if (!assigned) {
        if (checkpointed) {
          loadCheckpoint[T](checkpointPath, spark) match {
            case Some(resultDataset) => assignSparkResults(resultDataset)
            case None =>
              val resultDataset = computeDataset(spark)
              saveCheckpoint(checkpointPath, resultDataset)
              loadCheckpoint[T](checkpointPath, spark) match {
                case Some(ds) => assignSparkResults(ds)
                case None => throw new RuntimeException(s"failed to persist to: $checkpointPath")
              }
          }
        } else {
          assignComputedSparkResults(spark)
        }
        if (autoCachingEnabled && children.size >= 2) {
          dataset.cache()
        }
      }
    }
    dataset
  }

  def checkpointPath = new File(checkpointDir, getSignature).toString

  def assignSparkResults(resultDataset: Dataset[T]) = {
    synchronized {
      assert(!assigned)
      this.dataset = resultDataset
      assigned = true
    }
  }

  def assignComputedSparkResults(spark: SparkSession) = {
    synchronized {
      assert(!assigned)
      val resultDataset = computeDataset(spark)
      assignSparkResults(resultDataset)
    }
  }


  //  Actions

  def foreach(f: T => Unit): DR[Unit] = {
    this.mapToResult(_.foreach(f))
  }

  def foreachPartition(f: Iterator[T] => Unit): DR[Unit] = {
    this.mapToResult(_.foreachPartition(f))
  }

  def collect: DR[Array[T]] = {
    this.mapToResult(_.collect)
  }

  def toLocalIterator: DR[Iterator[T]] = {
    this.mapToResult(_.toLocalIterator)
  }

  def reduce(f: (T, T) => T): DR[T] = {
    this.mapToResult(_.reduce(f))
  }

  def treeReduce(f: (T, T) => T, depth: Int = 2): DR[T] = {
    this.mapToResult(_.treeReduce(f, depth))
  }

  def fold(zeroValue: T)(op: (T, T) => T): DR[T] = {
    this.mapToResult(_.fold(zeroValue)(op))
  }

  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): DR[U] = {
    this.mapToResult(_.aggregate(zeroValue)(seqOp, combOp))
  }

  def treeAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U,
                                               combOp: (U, U) => U,
                                               depth: Int = 2): DR[U] = {
    this.mapToResult(_.treeAggregate(zeroValue)(seqOp, combOp, depth))
  }

  def count: DR[Long] = {
    this.mapToResult(_.count)
  }

  def countByValue: DR[Map[T, Long]] = {
    this.mapToResult(_.countByValue)
  }

  //  Experimental
  def countApproxDistinct(p: Int, sp: Int): DR[Long] = {
    this.mapToResult(_.countApproxDistinct(p, sp))
  }

  def countApproxDistinct(relativeSD: Double = 0.05): DR[Long] = {
    this.mapToResult(_.countApproxDistinct(relativeSD))
  }

  def take(num: Int): DR[Array[T]] = {
    this.mapToResult(_.take(num))
  }

  def first: DR[T] = {
    this.mapToResult(_.first)
  }

  def top(num: Int)(implicit ord: Ordering[T]): DR[Array[T]] = {
    this.mapToResult(_.top(num))
  }

  def takeOrdered(num: Int)(implicit ord: Ordering[T]): DR[Array[T]] = {
    this.mapToResult(_.takeOrdered(num))
  }

  def max()(implicit ord: Ordering[T]): DR[T] = {
    this.mapToResult(_.max)
  }

  def min()(implicit ord: Ordering[T]): DR[T] = {
    this.mapToResult(_.min)
  }

  def isEmpty: DR[Boolean] = {
    this.mapToResult(_.isEmpty())
  }

}

object DC {

  implicit def dcToPairDCFunctions[K: Encoder, V: Encoder](dc: DC[(K, V)])
                                                          (implicit kt: ClassTag[K],
                                                           vt: ClassTag[V],
                                                           ord: Ordering[K] = null,
                                                           kSeqEncoder: Encoder[(K, Seq[V])],
                                                           kArrEncoder: Encoder[(K, Array[V])]): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def dcToSecondaryPairDCFunctions[K, K2, V](dc: DC[((K, K2), V)])
                                                     (implicit kt: ClassTag[K], k2t: ClassTag[K2], vt: ClassTag[V], ord: Ordering[(K, K2)] = null,
                                                      kk2vEncoder: Encoder[((K, K2), V)]): SecondaryPairDCFunctions[K, K2, V] = {
    new SecondaryPairDCFunctions(dc)
  }

  implicit def dcToDoubleFunctions(dc: DC[Double]): DoubleDCFunctions = {
    new DoubleDCFunctions(dc)
  }

}
