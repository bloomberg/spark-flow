package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by ngoehausen on 5/2/16.
  */
class MultiDFTransformDC(left: DC[Row], right: DC[Row],
                         f: (DataFrame, DataFrame) => DataFrame, hashTarget: Seq[String]) extends DC[Row](Seq(left, right)){

  def computeSparkResults(sc: SparkContext) = {
    val df = f(left.getDF(sc), right.getDF(sc))
    (df.rdd, Some(df.schema))
  }

  override def computeSignature() = {
    hashString(left.getSignature + right.getSignature + hashSeq(hashTarget))
  }

}
