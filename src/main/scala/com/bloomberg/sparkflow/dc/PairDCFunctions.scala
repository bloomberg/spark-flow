package com.bloomberg.sparkflow.dc

import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.{Partitioner, HashPartitioner}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
/**
  * Created by ngoehausen on 4/19/16.
  */
class PairDCFunctions[K,V](self: DC[(K,V)])
    (implicit kt: ClassTag[K],
     vt: ClassTag[V],
     ord: Ordering[K] = null,
     kEncoder: Encoder[K],
     vEncoder: Encoder[V],
     kvEncoder: Encoder[(K,V)],
     klEncoder: Encoder[(K,Long)],
     kSeqEncoder: Encoder[(K,Seq[V])],
     kArrEncoder: Encoder[(K,Array[V])]) {

  def reduceByKey(func: (V, V) => V): DC[(K, V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.reduceByKey(func), func)
  }

  def reduceByKey(func: (V, V) => V, numPartitions: Int): DC[(K, V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.reduceByKey(func, numPartitions), func, Seq(numPartitions.toString))
  }

  def countApproxDistinctByKey(relativeSD: Double = 0.05): DC[(K, Long)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.countApproxDistinctByKey(relativeSD), Seq("countApproxDistinctByKey", relativeSD.toString))
  }

  def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): DC[(K, Long)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.countApproxDistinctByKey(relativeSD, numPartitions), Seq("countApproxDistinctByKey", relativeSD.toString, numPartitions.toString))
  }

  def groupByKey(): DC[(K, Seq[V])] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.groupByKey().map{case (k, values) => (k, values.toSeq)}, Seq("groupByKey"))
  }

  def groupByKey(numPartitions: Int): DC[(K, Seq[V])] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.groupByKey(numPartitions).map{case (k, values) => (k, values.toSeq)}, Seq("groupByKey","groupByKey", numPartitions.toString))
  }


  def join[W](other: DC[(K,W)])(implicit kvwEncoder: Encoder[(K,(V,W))]): DC[(K, (V, W))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.join(right)
    }
    new MultiInputPairDC[(K, (V, W)), K](Seq(self, other), resultFunc)
  }

  def join[W](other: DC[(K,W)], numPartitions: Int)(implicit kvwEncoder: Encoder[(K,(V,W))]): DC[(K, (V, W))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.join(right, numPartitions)
    }
    new MultiInputPairDC[(K, (V, W)), K](Seq(self, other), resultFunc)
  }

  def leftOuterJoin[W](other: DC[(K,W)])(implicit kvwEncoder: Encoder[(K,(V,Option[W]))]): DC[(K, (V, Option[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.leftOuterJoin(right)
    }
    new MultiInputPairDC[(K, (V, Option[W])), K](Seq(self, other), resultFunc)
  }

  def leftOuterJoin[W](other: DC[(K,W)], numPartitions: Int)(implicit kvwEncoder: Encoder[(K,(V,Option[W]))]): DC[(K, (V, Option[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.leftOuterJoin(right, numPartitions)
    }
    new MultiInputPairDC[(K, (V, Option[W])), K](Seq(self, other), resultFunc)
  }

  def rightOuterJoin[W](other: DC[(K,W)])(implicit kvwEncoder: Encoder[(K,(Option[V],W))]): DC[(K, (Option[V], W))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.rightOuterJoin(right)
    }
    new MultiInputPairDC[(K, (Option[V], W)), K](Seq(self, other), resultFunc)
  }

  def rightOuterJoin[W](other: DC[(K,W)], numPartitions: Int)(implicit kvwEncoder: Encoder[(K,(Option[V],W))]): DC[(K, (Option[V], W))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.rightOuterJoin(right, numPartitions)
    }
    new MultiInputPairDC[(K, (Option[V], W)), K](Seq(self, other), resultFunc)
  }

  def fullOuterJoin[W](other: DC[(K,W)])(implicit kvwEncoder: Encoder[(K,(Option[V],Option[W]))]): DC[(K, (Option[V], Option[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.fullOuterJoin(right)
    }
    new MultiInputPairDC[(K, (Option[V], Option[W])), K](Seq(self, other), resultFunc)
  }

  def fullOuterJoin[W](other: DC[(K,W)], numPartitions: Int)(implicit kvwEncoder: Encoder[(K,(Option[V],Option[W]))]): DC[(K, (Option[V], Option[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.fullOuterJoin(right, numPartitions)
    }
    new MultiInputPairDC[(K, (Option[V], Option[W])), K](Seq(self, other), resultFunc)
  }
//
  def cogroup[W](other: DC[(K,W)])
                         (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W]))]): DC[(K, (Seq[V], Seq[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.cogroup(right).map{case (k, (v,w)) => (k, (v.toSeq, w.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W]))), K](Seq(self, other), resultFunc)
  }
//
  def cogroup[W1, W2](other1: DC[(K,W1)], other2: DC[(K,W2)])
                     (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2]))])
  : DC[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      first.cogroup(second, third).map{case (k, (v,w1,w2)) => (k, (v.toSeq, w1.toSeq, w2.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W1], Seq[W2]))), K](Seq(self, other1, other2), resultFunc)
  }

  def cogroup[W1, W2, W3](other1: DC[(K,W1)], other2: DC[(K,W2)], other3: DC[(K,W3)])
                         (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))]):
  DC[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      val fourth = rdds(3).asInstanceOf[RDD[(K,W3)]]
      first.cogroup(second, third, fourth).map{case (k, (v,w1,w2,w3)) => (k, (v.toSeq, w1.toSeq, w2.toSeq, w3.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))), K](Seq(self, other1, other2, other3), resultFunc)
  }

  def cogroup[W:ClassTag](other: DC[(K,W)], numPartitions: Int)
                         (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W]))]): DC[(K, (Seq[V], Seq[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.cogroup(right, numPartitions).map{case (k, (v,w)) => (k, (v.toSeq, w.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W]))), K](Seq(self, other), resultFunc)
  }
//
  def cogroup[W1, W2](other1: DC[(K,W1)], other2: DC[(K,W2)], numPartitions: Int)
                     (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2]))])
  : DC[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      first.cogroup(second, third, numPartitions).map{case (k, (v,w1,w2)) => (k, (v.toSeq, w1.toSeq, w2.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W1], Seq[W2]))), K](Seq(self, other1, other2), resultFunc)
  }

  def cogroup[W1, W2, W3](other1: DC[(K,W1)], other2: DC[(K,W2)], other3: DC[(K,W3)], numPartitions: Int)
                         (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))]):
  DC[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      val fourth = rdds(3).asInstanceOf[RDD[(K,W3)]]
      first.cogroup(second, third, fourth, numPartitions).map{case (k, (v,w1,w2,w3)) => (k, (v.toSeq, w1.toSeq, w2.toSeq, w3.toSeq))}
    }
    new MultiInputPairDC[((K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))), K](Seq(self, other1, other2, other3), resultFunc)
  }


  def groupWith[W](other: DC[(K, W)])(implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W]))]): DC[(K, (Seq[V], Seq[W]))] = {
    this.cogroup(other)
  }

  def groupWith[W1, W2](other1: DC[(K,W1)], other2: DC[(K,W2)])(implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2]))])
  : DC[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    this.cogroup(other1, other2)
  }

  def groupWith[W1, W2, W3](other1: DC[(K,W1)], other2: DC[(K,W2)], other3: DC[(K,W3)])
                           (implicit kvwEncoder: Encoder[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))]): DC[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))] = {
    this.cogroup(other1, other2, other3)
  }

  def subtractByKey[W: ClassTag](other: DC[(K,W)]): DC[(K,V)] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.subtractByKey(right)
    }
    new MultiInputPairDC[(K,V), K](Seq(self, other), resultFunc)
  }

  def subtractByKey[W: ClassTag](other: DC[(K,W)], numPartitions: Int): DC[(K,V)] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.subtractByKey(right, numPartitions)
    }
    new MultiInputPairDC[(K,V), K](Seq(self, other), resultFunc)
  }

  def keys: DC[K] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.keys, Seq("keys"))
  }

  def values: DC[V] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.values, Seq("values"))
  }

  def partitionBy(partitioner: Partitioner): DC[(K,V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.partitionBy(partitioner), Seq("partitionBy", partitioner.numPartitions.toString))
  }

  def partitionByKey(): DC[(K,V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.partitionBy(new HashPartitioner(rdd.partitions.length)), Seq("partitionByKey"))
  }

  def repartitionAndSortWithinPartitions(partitioner: Partitioner): DC[(K, V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.repartitionAndSortWithinPartitions(partitioner), Seq("repartitionAndSortWithinPartitions", partitioner.numPartitions.toString))
  }

  def sortByKey(ascending: Boolean = true): DC[(K,V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.sortByKey(ascending), Seq("sortByKey", ascending.toString))
  }

  def sortByKey(ascending: Boolean, numPartitions: Int): DC[(K,V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.sortByKey(ascending, numPartitions), Seq("sortByKey", ascending.toString, numPartitions.toString))
  }

}
