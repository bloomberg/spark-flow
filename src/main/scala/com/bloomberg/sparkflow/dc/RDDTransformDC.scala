package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Hashing._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/23/16.
  */
private[sparkflow] class RDDTransformDC[U:ClassTag, T:ClassTag]
(val prev: DC[T],
 f: RDD[T] => RDD[U],
 hashTarget: Seq[String]) extends DC[U](Seq(prev)) {

  def this(prev: DC[T], f: RDD[T] => RDD[U], hashTarget: AnyRef) = {
    this(prev, f, Seq(hashClass(hashTarget)))
  }

  def computeSparkResults(sc: SparkContext) = {
    val rdd = f(prev.getRDD(sc))
    (rdd, prev.getSchema(sc))
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTarget))
  }

}
