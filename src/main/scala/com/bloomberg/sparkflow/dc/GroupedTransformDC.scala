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
