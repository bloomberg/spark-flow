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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputPairDC[T: ClassTag, K: ClassTag](tEncoder: Encoder[T], inputs: Seq[DC[_ <: Product2[K, _]]],
                                                 f: (Seq[RDD[_ <: Product2[K, _]]]) => RDD[T])
  extends DC[T](tEncoder, inputs) {

  override def computeSignature() = {
    Hashing.hashString(inputs.map(_.getSignature).mkString("") + Hashing.hashClass(f))
  }

  override def computeDataset(spark: SparkSession) = {
    val rddFutures = inputs.map(dc => Future{dc.getRDD(spark)})
    val rdds = rddFutures.map(rddFuture => Await.result(rddFuture, Duration.Inf))
    val rdd = f(rdds)
    spark.createDataset(rdd)
  }


}
