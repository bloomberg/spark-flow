package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/20/16.
  */
class KeyValueGroupedDCImpl[K: ClassTag, V, T]
(prev: DC[T],
 fGroups: (Dataset[T]) => KeyValueGroupedDataset[K, V],
 hashTargets: Seq[String])
(implicit kEncoder: Encoder[K],
 vEncoder: Encoder[V],
 kvEncoder: Encoder[(K, V)]) extends KeyValueGroupedDC[K, V](prev) {

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTargets) + hashClass(fGroups))
  }

  private var keyValueGroupedDataset: KeyValueGroupedDataset[K, V] = _

  def get(spark: SparkSession) = {
    if (keyValueGroupedDataset == null) {
      keyValueGroupedDataset = computeKeyValueGroupedDataset(spark)
    }
    keyValueGroupedDataset
  }

  private def computeKeyValueGroupedDataset(spark: SparkSession) = {
    fGroups(prev.getDataset(spark))
  }

  override def keyAs[L: Encoder : ClassTag](implicit lvEncoder: Encoder[(L, V)]): KeyValueGroupedDC[L, V] = {
    new KeyValueGroupedDCImpl[L, V, T](prev, (ds: Dataset[T]) => fGroups(ds).keyAs[L], hashTargets)
  }

  //  def keys: DC[K] = {
  //    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K,V]) => kv.keys)
  //  }
  //
  //  def flatMapGroups[U : Encoder: ClassTag](f: (K, Iterator[V]) => TraversableOnce[U]): DC[U] = {
  //    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K,V]) => kv.flatMapGroups(f))
  //  }
  //
  //  def mapGroups[U : Encoder : ClassTag](f: (K, Iterator[V]) => U): DC[U] = {
  //    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K,V]) => kv.mapGroups(f))
  //  }
  //
  //  def reduceGroups(f: (V, V) => V): DC[(K, V)] = {
  //    new GroupedTransformDC[K,V, (K,V)](this, (kv: KeyValueGroupedDataset[K,V]) => kv.reduceGroups(f))
  //  }
  //
  //  def agg[U1](col1: TypedColumn[V, U1])(implicit kUEncoder: Encoder[(K,U1)]): DC[(K, U1)] = {
  //    new GroupedTransformDC[K,V, (K,U1)](this, (kv: KeyValueGroupedDataset[K,V]) => kv.agg(col1))
  //  }
  //
  //  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2])(implicit kuEncoder: Encoder[(K,U1,U2)]): DC[(K, U1, U2)] = {
  //    new GroupedTransformDC[K,V, (K,U1,U2)](this, (kv: KeyValueGroupedDataset[K,V]) => kv.agg(col1, col2))
  //  }
  //
  //  def count(implicit kLEncoder: Encoder[(K,Long)]): DC[(K, Long)] = {
  //    new GroupedTransformDC[K,V, (K, Long)](this, (kv: KeyValueGroupedDataset[K,V]) => kv.count)
  //  }
  //
  //  def cogroup[U, R : Encoder : ClassTag](
  //                               other: KeyValueGroupedDC[K, U])(
  //                               f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): DC[R] = {
  //    val func = (left: KeyValueGroupedDataset[K,V], right: KeyValueGroupedDataset[K,U]) => {
  //      left.cogroup(right)(f)
  //    }
  //    new MultiGroupedTransformDC[K,V,U,R](this, other, func)


}
