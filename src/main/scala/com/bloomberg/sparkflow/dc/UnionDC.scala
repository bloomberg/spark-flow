package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by rely10 on 5/27/16.
  */
class UnionDC[T: ClassTag](left: DC[T], right: DC[T]) extends DC[T](Seq(left, right)){

  override def computeSignature() = {
    Hashing.hashSeq(Seq("union", left.getSignature, right.getSignature))
  }

  override def computeSparkResults(sc: SparkContext) = {
    val rdd = left.getRDD(sc).union(right.getRDD(sc))
    val leftSchema = left.getSchema(sc)
    val rightSchema = right.getSchema(sc)
    val resultSchema = if (leftSchema == rightSchema) leftSchema else None
//    TODO: perhaps throw error if schemas defined and unequal
    (rdd, resultSchema)
  }

}
