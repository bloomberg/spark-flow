package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.reflect.ClassTag
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 3/23/16.
  */
class SourceDC[T: ClassTag](val source: String, val sourceFunc: SparkContext => RDD[T], val sourceType: String)(implicit tEncoder: Encoder[T]) extends DC[T](Nil)  {

  def computeDataset(spark: SparkSession) = {
    sourceFunc(spark.sparkContext).toDS()
  }

  override def computeSignature() = {
    Hashing.hashString(s"$sourceType:$source")
  }

}