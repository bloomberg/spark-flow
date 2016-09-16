package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * Created by ngoehausen on 3/23/16.
  */
private[sparkflow] class RDDTransformDC[U, T]
(uEncoder: Encoder[U], val prev: DC[T], f: (RDD[T]) => RDD[U], hashTarget: Seq[String]) extends DC[U](uEncoder, Seq(prev)) {

  def this(uEncoder: Encoder[U], prev: DC[T], f: RDD[T] => RDD[U], hashTarget: AnyRef) = {
    this(uEncoder, prev, f, Seq(hashClass(hashTarget)))
  }

  def this(uEncoder: Encoder[U], prev: DC[T], f: RDD[T] => RDD[U], hashTarget: AnyRef, hashTargets: Seq[String]) = {
    this(uEncoder, prev, f, hashClass(hashTarget) +: hashTargets)
  }

  def computeDataset(spark: SparkSession) = {
    val rdd = f(prev.getRDD(spark))
    spark.createDataset(rdd)
  }

  def this(uEncoder: Encoder[U], prev: DC[T], f: RDD[T] => RDD[U], functionHashTargets: Seq[AnyRef], StringHashTargets: Seq[String]) = {
    this(uEncoder, prev, f, functionHashTargets.map(hashClass).mkString("") +: StringHashTargets)
  }


  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTarget))
  }

}
