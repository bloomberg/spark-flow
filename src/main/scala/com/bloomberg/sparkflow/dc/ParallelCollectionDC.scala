package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T:ClassTag](val data: Seq[T], numSlices: Option[Int]) extends DC[T](Nil) {

  def this(data: Seq[T]) = this(data, None)

  override def computeSparkResults(sc: SparkContext) = {
    numSlices match {
      case Some(n) => (sc.parallelize(data, n), None)
      case None => (sc.parallelize(data), None)
    }
  }

  override def computeSignature() = {
    Hashing.hashString(data.map(_.toString).reduce(_ + _))
  }


}
