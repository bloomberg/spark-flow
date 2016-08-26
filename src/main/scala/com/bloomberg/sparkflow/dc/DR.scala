package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.getSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Deferred Result
  */
abstract class DR[U](dep: DC[_]) extends Dependency[U](Seq(dep)) {

  def get(spark: SparkSession): U

  def get(sc: SparkContext): U = {
    get(getSpark(sc))
  }

}