package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.sql.{SparkSession, Encoder, Dataset, KeyValueGroupedDataset}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/28/16.
  */
class MultiGroupedTransformDC[K,V,U,T: ClassTag]
(left: KeyValueGroupedDC[K,V],
 right: KeyValueGroupedDC[K,U],
 f: (KeyValueGroupedDataset[K,V], KeyValueGroupedDataset[K,U]) => Dataset[T])
(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(left, right)){

  override def computeDataset(spark: SparkSession) = {
    val dataset = f(left.get(spark), right.get(spark))
    dataset
  }

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f) )
  }

}
