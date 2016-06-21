package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{SparkSession, KeyValueGroupedDataset, Dataset, Encoder}

/**
  * Created by ngoehausen on 6/20/16.
  */
class KeyValueGroupedDCImpl[K, V, T]
(kEncoder: Encoder[K],
 prev: DC[T],
 f: (Dataset[T]) => KeyValueGroupedDataset[K,V],
 hashTargets: Seq[String]) extends KeyValueGroupedDC[K,V](prev){


  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTargets) + hashClass(f))
  }

  private var keyValueGroupedDataset: KeyValueGroupedDataset[K,V] = _

  def get(spark: SparkSession) = {
    if (keyValueGroupedDataset == null){
      keyValueGroupedDataset = computeKeyValueGroupedDataset(spark)
    }
    keyValueGroupedDataset
  }

  private def computeKeyValueGroupedDataset(spark: SparkSession) = {
    f(prev.getDataset(spark))
  }
  //
  //  def flatMapGroups[U : Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): DC[U] = {
  //    new DatasetTransformDC()
  //
  //  }


}
