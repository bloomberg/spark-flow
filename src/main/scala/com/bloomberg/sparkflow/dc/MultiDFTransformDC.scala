package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

/**
  * Created by ngoehausen on 5/2/16.
  */
class MultiDFTransformDC(left: DC[Row], right: DC[Row],
                         f: (DataFrame, DataFrame) => DataFrame, hashTarget: Seq[String])(implicit rEncoder: Encoder[Row]) extends DC[Row](rEncoder, Seq(left, right)) {

  def computeDataset(spark: SparkSession) = {
    val df = f(left.getDF(spark), right.getDF(spark))
    df
  }

  override def computeSignature() = {
    hashString(left.getSignature + right.getSignature + hashSeq(hashTarget))
  }

}
