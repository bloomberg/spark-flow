package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Hashing._
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class DRImpl[T: ClassTag, U: ClassTag](prev: DC[T], f: RDD[T] => U) extends DR[U](prev){

  private var result: U = _

  override def get(spark: SparkSession) = {
    if (result == null){
      result = computeResult(spark)
    }
    result
  }

  private def computeResult(spark: SparkSession) = {
    f(prev.getRDD(spark))
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashClass(f))
  }

}
