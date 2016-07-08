package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.partitioning.SecondarySortPartioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/6/16.
  */
class SecondaryPairDCFunctions[K,K2,V](self: DC[((K,K2),V)])
                                   (implicit kt: ClassTag[K],
                                    vt: ClassTag[V],
                                    k2t: ClassTag[K2],
                                    ord: Ordering[(K,K2)] = null,
                                    encoder: Encoder[((K,K2), V)])  {

  def repartitionAndSecondarySortWithinPartitions(): DC[((K,K2),V)] = {
    new RDDTransformDC(self, (rdd: RDD[((K,K2), V)]) => rdd.repartitionAndSortWithinPartitions(new SecondarySortPartioner[K,K2,V](rdd.partitions.length)), Seq("repartAndSort"))
  }

  def repartitionAndSecondarySortWithinPartitions(numPartitions: Int): DC[((K,K2),V)] = {
    new RDDTransformDC(self, (rdd: RDD[((K,K2), V)]) => rdd.repartitionAndSortWithinPartitions(new SecondarySortPartioner[K,K2,V](numPartitions)), Seq("repartAndSort", numPartitions.toString))
  }
}
