package com.bloomberg.sparkflow.dc

import org.apache.spark.sql.{Encoder, Dataset, KeyValueGroupedDataset, SparkSession}

/**
  * Created by ngoehausen on 6/13/16.
  */
abstract class KeyValueGroupedDC[K, V]
(prev: DC[_]) extends Dependency[(K,V)](Seq(prev)){

  def get(spark: SparkSession): KeyValueGroupedDataset[K,V]

}
