package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputDC[T:ClassTag, U:ClassTag, V:ClassTag](left: DC[U], right: DC[V],
                                             f: (RDD[U], RDD[V]) => RDD[T])
  extends DC[T](Seq(left, right)){

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f))
  }

  override def computeSparkResults(sc: SparkContext) = {
    val rdd = f(left.getRDD(sc), right.getRDD(sc))
    (rdd, None)
  }


}
