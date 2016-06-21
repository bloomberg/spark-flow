package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{SparkSession, Dataset, Encoder}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/13/16.
  */
private[sparkflow] class DatasetTransformDC[U: ClassTag, T: ClassTag]
(val prev: DC[T],
 f: Dataset[T] => Dataset[U],
 hashTargets: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U]) extends DC[U](Seq(prev)) {

  def this(prev: DC[T], f: Dataset[T] => Dataset[U], hashTarget: AnyRef)(implicit tEncoder: Encoder[T], uEncoder: Encoder[U])  = {
    this(prev, f, Seq(hashClass(hashTarget)))
  }

  def this(prev: DC[T], f: Dataset[T] => Dataset[U], hashTarget: AnyRef, hashTargets: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U])  = {
    this(prev, f, hashClass(hashTarget) +: hashTargets)
  }

  def computeDataset(spark: SparkSession) = {
    val dataset = f(prev.getDataset(spark))
    dataset
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTargets))
  }

}
