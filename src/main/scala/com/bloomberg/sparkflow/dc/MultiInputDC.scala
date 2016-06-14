package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row, Encoder}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputDC[T:ClassTag, U:ClassTag, V:ClassTag](left: DC[U], right: DC[V],
                                             f: (RDD[U], RDD[V]) => RDD[T])(implicit tEncoder: Encoder[T])
  extends DC[T](Seq(left, right)){

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f))
  }

  override def computeDataset(spark: SparkSession) = {
    val rdd = f(left.getRDD(spark), right.getRDD(spark))
    spark.createDataset(rdd)
  }


}
