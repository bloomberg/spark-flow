package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import Hashing._

import scala.reflect.ClassTag

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U:ClassTag, T:ClassTag]
(val prev: DC[T], dr: DR[U]) extends DC[(T,U)](Seq(prev, dr)) {

  override def computeSparkResults(sc: SparkContext) = {
    val result = dr.get(sc)
    val rdd = prev.getRDD(sc).mapPartitions(iterator => {
      iterator.map(t => (t, result))
    })
    (rdd, None)
  }

  override def computeSignature() = {
    hashString(prev.getSignature + dr.getSignature)
  }

}
