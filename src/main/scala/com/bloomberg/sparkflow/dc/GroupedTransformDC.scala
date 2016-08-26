package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset, SparkSession}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/20/16.
  */
class GroupedTransformDC[K, V, T: ClassTag]
(prev: KeyValueGroupedDC[K, V],
 f: KeyValueGroupedDataset[K, V] => Dataset[T])(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(prev)) {

  override def computeDataset(spark: SparkSession) = {
    val dataset = f(prev.get(spark))
    dataset
  }

  override def computeSignature() = {
    Hashing.hashString(prev.getSignature + Hashing.hashClass(f))
  }

}
