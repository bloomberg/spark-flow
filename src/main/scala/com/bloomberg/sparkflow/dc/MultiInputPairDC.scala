package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputPairDC[T: ClassTag, K: ClassTag](tEncoder: Encoder[T], inputs: Seq[DC[_ <: Product2[K, _]]],
                                                 f: (Seq[RDD[_ <: Product2[K, _]]]) => RDD[T])
  extends DC[T](tEncoder, inputs) {

  override def computeSignature() = {
    Hashing.hashString(inputs.map(_.getSignature).mkString("") + Hashing.hashClass(f))
  }

  override def computeDataset(spark: SparkSession) = {
    val rddFutures = inputs.map(dc => Future{dc.getRDD(spark)})
    val rdds = rddFutures.map(rddFuture => Await.result(rddFuture, Duration.Inf))
    val rdd = f(rdds)
    spark.createDataset(rdd)
  }


}
