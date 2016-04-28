package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import Hashing._

import scala.reflect.ClassTag

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U:ClassTag, T:ClassTag, V: ClassTag]
(val prev: DC[T], dr: DR[U],f: (T,U) => V) extends DC[V](Seq(prev, dr)) {

  override def computeSparkResults(sc: SparkContext) = {
    val result = dr.get(sc)
    val rdd = prev.getRDD(sc).mapPartitions(iterator => {
      iterator.map(t => f(t, result))
    })
    (rdd, None)
  }

  override def computeSignature() = {
    hashString(prev.getSignature + dr.getSignature + hashClass(f))
  }

}
