package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import Hashing._
import org.apache.spark.sql.{SparkSession, Encoder}

import scala.reflect.ClassTag

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U:ClassTag, T:ClassTag]
(val prev: DC[T], dr: DR[U])(implicit tuEncoder: Encoder[(T,U)]) extends DC[(T,U)](Seq(prev, dr)) {

  override def computeDataset(spark: SparkSession) = {
    val result = dr.get(spark)
    prev.getDataset(spark).mapPartitions(iterator => {
      iterator.map(t => (t, result))
    })
  }

  override def computeSignature() = {
    hashString(prev.getSignature + dr.getSignature)
  }

}
