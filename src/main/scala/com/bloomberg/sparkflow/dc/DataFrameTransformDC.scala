package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by ngoehausen on 4/28/16.
  */
class DataFrameTransformDC(prev: DC[Row], f: DataFrame => DataFrame, hashTarget: Seq[String]) extends DC[Row](emptyRowEncoder, Seq(prev)) {

  def computeDataset(spark: SparkSession) = {
    val df = f(prev.getDF(spark))
    df
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTarget))
  }

}
