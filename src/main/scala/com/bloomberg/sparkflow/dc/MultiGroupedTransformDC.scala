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

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by ngoehausen on 6/28/16.
  */
class MultiGroupedTransformDC[K, V, U, T: ClassTag]
(left: KeyValueGroupedDC[K, V],
 right: KeyValueGroupedDC[K, U],
 f: (KeyValueGroupedDataset[K, V], KeyValueGroupedDataset[K, U]) => Dataset[T])
(implicit tEncoder: Encoder[T]) extends DC[T](tEncoder, Seq(left, right)) {

  override def computeDataset(spark: SparkSession) = {
    val leftFuture = Future{left.get(spark)}
    val rightFuture = Future{right.get(spark)}
    val ld = Await.result(leftFuture, Duration.Inf)
    val rd = Await.result(rightFuture, Duration.Inf)
    val dataset = f(ld, rd)
    dataset
  }

  override def computeSignature() = {
    Hashing.hashString(left.getSignature + right.getSignature + Hashing.hashClass(f))
  }

}
