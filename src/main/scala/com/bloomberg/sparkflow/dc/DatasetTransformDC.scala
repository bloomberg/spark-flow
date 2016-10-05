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

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
  * Created by ngoehausen on 6/13/16.
  */
private[sparkflow] class DatasetTransformDC[U, T]
(encoder: Encoder[U], val prev: DC[T], f: (Dataset[T]) => Dataset[U], hashTargets: Seq[String]) extends DC[U](encoder, Seq(prev)) {
  //
  //  def this(prev: DC[T], f: Dataset[T] => Dataset[U], hashTarget: AnyRef)(implicit tEncoder: Encoder[T], uEncoder: Encoder[U])  = {
  //    this(prev, uEncoder, f, Seq(hashClass(hashTarget)))
  //  }
  //
  //  def this(prev: DC[T], f: Dataset[T] => Dataset[U], hashTarget: AnyRef, hashTargets: Seq[String])(implicit tEncoder: Encoder[T], uEncoder: Encoder[U])  = {
  //    this(prev,uEncoder,  f, hashClass(hashTarget) +: hashTargets)
  //  }

  def computeDataset(spark: SparkSession) = {
    val dataset = f(prev.getDataset(spark))
    dataset
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashSeq(hashTargets))
  }

}
