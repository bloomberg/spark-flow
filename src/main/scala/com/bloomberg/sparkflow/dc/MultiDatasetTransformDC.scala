package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql._
import com.bloomberg.sparkflow.emptyRowEncoder

/**
  * Created by ngoehausen on 5/2/16.
  */
class MultiDatasetTransformDC(left: DC[_], right: DC[_],
                              f: (Dataset[_], Dataset[_]) => DataFrame, hashTarget: Seq[String]) extends DC[Row](emptyRowEncoder, Seq(left, right)) {

  def computeDataset(spark: SparkSession) = {
    val df = f(left.getDF(spark), right.getDF(spark))
    df
  }

  override def computeSignature() = {
    hashString(left.getSignature + right.getSignature + hashSeq(hashTarget))
  }

}
