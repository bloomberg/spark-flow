package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by ngoehausen on 6/28/16.
  */
class MultiGroupedTransformDC[K, V, U, T: ClassTag]
(left: KeyValueGroupedDC[K, V],
 right: KeyValueGroupedDC[K, U],
 f: (KeyValueGroupedDataset[K, V], KeyValueGroupedDataset[K, U]) => Dataset[T])
(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(left, right)) {

  override def computeDataset(spark: SparkSession) = {
    val leftFuture = Future{left.get(spark)}
    val rightFuture = Future{right.get(spark)}
    val ld = Await.result(leftFuture, Duration.Inf)
    val rd = Await.result(rightFuture, Duration.Inf)
    val dataset = f(ld, rd)
    dataset
  }

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f))
  }

}
