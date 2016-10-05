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
import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U, T]
(encoder: Encoder[(T, U)], val prev: DC[T], dr: DR[U]) extends DC[(T, U)](encoder, Seq(prev, dr)) {

  override def computeDataset(spark: SparkSession) = {
    val result = dr.get(spark)
    prev.getDataset(spark).mapPartitions(iterator => {
      iterator.map(t => (t, result))
    })
  }

  override def computeSignature() = {
    hashString(prev.getSignature + dr.getSignature)
  }

}
