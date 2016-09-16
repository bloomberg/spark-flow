package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * Created by ngoehausen on 3/23/16.
  */
class SourceDC[T](encoder: Encoder[T], val source: String, val sourceFunc: SparkContext => RDD[T], val sourceType: String) extends DC[T](encoder, Nil) {

  def computeDataset(spark: SparkSession) = {
    sourceFunc(spark.sparkContext).toDS()
  }

  override def computeSignature() = {
    Hashing.hashString(s"$sourceType:$source")
  }

}