package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by rely10 on 5/27/16.
  */
class UnionDC[T](left: DC[T], right: DC[T])(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(left, right)) {

  override def computeSignature() = {
    Hashing.hashSeq(Seq("union", left.getSignature, right.getSignature))
  }

  def computeDataset(spark: SparkSession) = {
    val leftFuture = Future{left.getDataset(spark)}
    val rightFuture = Future{right.getDataset(spark)}
    val ld = Await.result(leftFuture, Duration.Inf)
    val rd = Await.result(rightFuture, Duration.Inf)
    ld.union(rd)
  }

}
