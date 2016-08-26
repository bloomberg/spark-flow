package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql._

/**
  * Created by ngoehausen on 4/26/16.
  */
class DataframeSourceDC(f: SparkSession => DataFrame, path: String, options: Map[String, String]) extends DC[Row](emptyRowEncoder, Nil) {

  override def computeDataset(spark: SparkSession) = {
    val dataFrame = f(spark)
    dataFrame
  }

  override def computeSignature() = {
    Hashing.hashString(Hashing.hashString(path) + Hashing.hashString(options.toSeq.sorted.toString))
  }

}
