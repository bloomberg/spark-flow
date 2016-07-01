package com.bloomberg.sparkflow.dc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import com.bloomberg.sparkflow.getSpark

/**
  * Deferred Result
  */
abstract class DR[U:ClassTag](dep: DC[_]) extends Dependency[U](Seq(dep)) {

  def get(spark: SparkSession): U

  def get(sc: SparkContext): U = {
    get(getSpark(sc))
  }

}