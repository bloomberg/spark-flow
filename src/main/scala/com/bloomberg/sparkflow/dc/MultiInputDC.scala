package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputDC[T, U, V](encoder: Encoder[T], left: DC[U], right: DC[V],
                            f: (RDD[U], RDD[V]) => RDD[T])
  extends DC[T](encoder, Seq(left, right)) {

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f))
  }

  override def computeDataset(spark: SparkSession) = {
    val leftFuture = Future{left.getRDD(spark)}
    val rightFuture = Future{right.getRDD(spark)}
    val leftRDD = Await.result(leftFuture, Duration.Inf)
    val rightRDD = Await.result(rightFuture, Duration.Inf)
    val rdd = f(leftRDD, rightRDD)
    spark.createDataset(rdd)
  }


}
