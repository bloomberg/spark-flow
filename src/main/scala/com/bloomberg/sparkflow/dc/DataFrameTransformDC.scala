package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

/**
  * Created by ngoehausen on 4/28/16.
  */
class DataFrameTransformDC(prev: DC[Row], f: DataFrame => DataFrame, hashTarget: Seq[String]) extends DC[Row](Seq(prev)){

  def computeSparkResults(sc: SparkContext) = {
    val df = f(prev.getDF(sc))
    (df.rdd, Some(df.schema))
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTarget))
  }

}
