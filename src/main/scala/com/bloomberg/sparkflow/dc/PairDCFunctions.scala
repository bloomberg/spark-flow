package com.bloomberg.sparkflow.dc

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class PairDCFunctions[K,V](self: DC[(K,V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null){

  def reduceByKey(func: (V, V) => V): DC[(K, V)] = {
    new RDDTransformDC(self, (rdd: RDD[(K,V)]) => rdd.reduceByKey(func), func)
  }

  def groupByKey(): DC[(K, Iterable[V])] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.groupByKey(), Seq("groupByKey"))
  }

  def groupByKey(numPartitions: Int): DC[(K, Iterable[V])] = {
    new RDDTransformDC(self, (rdd: RDD[(K, V)]) => rdd.groupByKey(numPartitions), Seq("groupByKey", numPartitions.toString))
  }

  def join[W](other: DC[(K,W)]): DC[(K, (V, W))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.join(right)
    }
    new MultiInputDC[(K, (V, W)), K](Seq(self, other), resultFunc)
  }

  def cogroup[W](other: DC[(K,W)]): DC[(K, (Iterable[V], Iterable[W]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val left = rdds(0).asInstanceOf[RDD[(K,V)]]
      val right = rdds(1).asInstanceOf[RDD[(K,W)]]
      left.cogroup(right)
    }
    new MultiInputDC[((K, (Iterable[V], Iterable[W]))), K](Seq(self, other), resultFunc)
  }

  def cogroup[W1, W2](other1: DC[(K,W1)], other2: DC[(K,W2)])
  : DC[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      first.cogroup(second, third)
    }
    new MultiInputDC[((K, (Iterable[V], Iterable[W1], Iterable[W2]))), K](Seq(self, other1, other2), resultFunc)
  }

  def cogroup[W1, W2, W3](other1: DC[(K,W1)], other2: DC[(K,W2)], other3: DC[(K,W3)]): DC[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    val resultFunc = (rdds: Seq[RDD[_ <: Product2[K, _]]]) => {
      val first = rdds(0).asInstanceOf[RDD[(K,V)]]
      val second = rdds(1).asInstanceOf[RDD[(K,W1)]]
      val third = rdds(2).asInstanceOf[RDD[(K,W2)]]
      val fourth = rdds(3).asInstanceOf[RDD[(K,W3)]]
      first.cogroup(second, third, fourth)
    }
    new MultiInputDC[((K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))), K](Seq(self, other1, other2, other3), resultFunc)
  }

}
