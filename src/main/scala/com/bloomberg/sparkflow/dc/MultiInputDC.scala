package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputDC[T: ClassTag, K:ClassTag](inputs: Seq[DC[_ <: Product2[K, _]]],
                                            f: (Seq[RDD[_ <: Product2[K, _]]]) => RDD[T])
  extends DC[T](inputs){

  override def computeSignature() = {
    Hashing.hashString(inputs.map(_.getSignature).mkString("") + Hashing.hashClass(f))
  }

  override def computeSparkResults(sc: SparkContext) = {
    val rdd = f(inputs.map(_.getRDD(sc)))
    (rdd, None)
  }


}
