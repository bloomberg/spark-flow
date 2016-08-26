package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * Created by rely10 on 5/27/16.
  */
class UnionDC[T](left: DC[T], right: DC[T])(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(left, right)) {

  override def computeSignature() = {
    Hashing.hashSeq(Seq("union", left.getSignature, right.getSignature))
  }

  def computeDataset(spark: SparkSession) = {
    left.getDataset(spark).union(right.getDataset(spark))
  }

}
