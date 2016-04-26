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
 hashTarget: AnyRef) extends DC[U](Seq(prev)) {

  def computeRDD(sc: SparkContext) = {
    f(prev.getRDD(sc))
  }

  override def computeHash() = {
    hashString(prev.getHash + hashClass(hashTarget))
  }


}
