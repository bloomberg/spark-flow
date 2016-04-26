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

  override def computeRDD(sc: SparkContext) = {
    val result = dr.get(sc)
    prev.getRDD(sc).mapPartitions(iterator => {
      iterator.map(t => f(t, result))
    })
  }

  override def computeHash() = {
    hashString(prev.getHash + dr.getHash + hashClass(f))
  }

}
