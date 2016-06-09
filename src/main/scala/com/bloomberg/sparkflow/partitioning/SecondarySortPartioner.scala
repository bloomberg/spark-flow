package com.bloomberg.sparkflow.partitioning

import org.apache.spark.Partitioner

/**
  * Created by ngoehausen on 6/6/16.
  */
class SecondarySortPartioner[K, K2, V](partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val (k, k2) = key.asInstanceOf[(K, K2)]
    // should be k.hashCode() mod numPartitions but java is dumb
    (k.hashCode() % numPartitions + numPartitions) % numPartitions
  }

}
