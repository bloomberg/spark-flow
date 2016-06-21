package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, Encoder}

import scala.reflect.ClassTag

import com.bloomberg.sparkflow._
/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T:ClassTag](val data: Seq[T], numSlices: Option[Int])(implicit tEncoder: Encoder[T]) extends DC[T](Nil) {

  def this(data: Seq[T])(implicit tEncoder: Encoder[T]) = this(data, None)


  def computeDataset(spark: SparkSession) = {
    numSlices match {
      case Some(n) => spark.sparkContext.parallelize(data, n).toDS()
      case None => spark.sparkContext.parallelize(data).toDS()
    }
  }

  override def computeSignature() = {
    Hashing.hashString(data.map(_.toString).reduce(_ + _))
  }


}
