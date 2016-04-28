package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T:ClassTag](val data: Seq[T]) extends DC[T](Nil) {

  override def computeSparkResults(sc: SparkContext) = (sc.parallelize(data), None)

  override def computeSignature() = {
    Hashing.hashString(data.map(_.toString).reduce(_ + _))
  }


}
