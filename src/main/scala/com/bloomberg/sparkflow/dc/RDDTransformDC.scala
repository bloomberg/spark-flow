package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Hashing._
import org.apache.spark.sql.{Encoder, Dataset, SparkSession}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/23/16.
  */
private[sparkflow] class RDDTransformDC[U:ClassTag, T:ClassTag]
(val prev: DC[T],
 f: RDD[T] => RDD[U],
 hashTarget: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U])  extends DC[U](Seq(prev)) {

  def this(prev: DC[T], f: RDD[T] => RDD[U], hashTarget: AnyRef)(implicit tEncoder: Encoder[T], uEncoder: Encoder[U]) = {
    this(prev, f, Seq(hashClass(hashTarget)))
  }

  def this(prev: DC[T], f: RDD[T] => RDD[U], hashTarget: AnyRef, hashTargets: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U]) = {
    this(prev, f, hashClass(hashTarget) +: hashTargets)
  }

  def computeDataset(spark: SparkSession) = {
    val rdd = f(prev.getRDD(spark))
    spark.createDataset(rdd)
  }

  def this(prev: DC[T], f: RDD[T] => RDD[U], functionHashTargets: Seq[AnyRef], StringHashTargets: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U]) = {
    this(prev, f, functionHashTargets.map(hashClass(_)).mkString("") +: StringHashTargets)
  }



  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTarget))
  }

}
