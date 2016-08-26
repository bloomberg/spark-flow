package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{Encoder, SparkSession}


/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T](encoder: Encoder[T], val data: Seq[T], numSlices: Option[Int]) extends DC[T](encoder, Nil) {


  def computeDataset(spark: SparkSession) = {
    numSlices match {
      case Some(n) => spark.createDataset(data).repartition(n)
      case None => spark.createDataset(data)
    }
  }

  override def computeSignature() = {
    Hashing.hashString(data.map(_.toString).reduce(_ + _))
  }


}
