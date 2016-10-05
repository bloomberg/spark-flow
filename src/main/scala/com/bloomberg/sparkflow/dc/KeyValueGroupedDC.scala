/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bloomberg.sparkflow.dc

import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/13/16.
  */
abstract class KeyValueGroupedDC[K: ClassTag, V]
(prev: DC[_])(implicit kEncoder: Encoder[K],
              vEncoder: Encoder[V],
              kvEncoder: Encoder[(K, V)]) extends Dependency[(K, V)](Seq(prev)) {

  def get(spark: SparkSession): KeyValueGroupedDataset[K, V]


  def keyAs[L: Encoder : ClassTag](implicit lvEncoder: Encoder[(L, V)]): KeyValueGroupedDC[L, V]

  def keys: DC[K] = {
    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K, V]) => kv.keys)
  }

  def flatMapGroups[U: Encoder : ClassTag](f: (K, Iterator[V]) => TraversableOnce[U]): DC[U] = {
    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K, V]) => kv.flatMapGroups(f))
  }

  def mapGroups[U: Encoder : ClassTag](f: (K, Iterator[V]) => U): DC[U] = {
    new GroupedTransformDC(this, (kv: KeyValueGroupedDataset[K, V]) => kv.mapGroups(f))
  }

  def reduceGroups(f: (V, V) => V): DC[(K, V)] = {
    new GroupedTransformDC[K, V, (K, V)](this, (kv: KeyValueGroupedDataset[K, V]) => kv.reduceGroups(f))
  }

  def agg[U1](col1: TypedColumn[V, U1])(implicit kUEncoder: Encoder[(K, U1)]): DC[(K, U1)] = {
    new GroupedTransformDC[K, V, (K, U1)](this, (kv: KeyValueGroupedDataset[K, V]) => kv.agg(col1))
  }

  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2])(implicit kuEncoder: Encoder[(K, U1, U2)]): DC[(K, U1, U2)] = {
    new GroupedTransformDC[K, V, (K, U1, U2)](this, (kv: KeyValueGroupedDataset[K, V]) => kv.agg(col1, col2))
  }

  def count(implicit kLEncoder: Encoder[(K, Long)]): DC[(K, Long)] = {
    new GroupedTransformDC[K, V, (K, Long)](this, (kv: KeyValueGroupedDataset[K, V]) => kv.count)
  }

  def cogroup[U, R: Encoder : ClassTag](
                                         other: KeyValueGroupedDC[K, U])(
                                         f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): DC[R] = {
    val func = (left: KeyValueGroupedDataset[K, V], right: KeyValueGroupedDataset[K, U]) => {
      left.cogroup(right)(f)
    }
    new MultiGroupedTransformDC[K, V, U, R](this, other, func)

  }
}
