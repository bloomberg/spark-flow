package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputPairDC[T: ClassTag, K:ClassTag](tEncoder: Encoder[T], inputs: Seq[DC[_ <: Product2[K, _]]],
                                                f: (Seq[RDD[_ <: Product2[K, _]]]) => RDD[T])
  extends DC[T](tEncoder, inputs){

  override def computeSignature() = {
    Hashing.hashString(inputs.map(_.getSignature).mkString("") + Hashing.hashClass(f))
  }

  override def computeDataset(spark: SparkSession) = {
    val rdd = f(inputs.map(_.getRDD(spark)))
    spark.createDataset(rdd)
  }


}
