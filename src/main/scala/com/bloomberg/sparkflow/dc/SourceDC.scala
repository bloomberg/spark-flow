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

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * Created by ngoehausen on 3/23/16.
  */
class SourceDC[T](encoder: Encoder[T], val source: String, val sourceFunc: SparkContext => RDD[T], val sourceType: String) extends DC[T](encoder, Nil) {

  def computeDataset(spark: SparkSession) = {
    sourceFunc(spark.sparkContext).toDS()
  }

  override def computeSignature() = {
    Hashing.hashString(s"$sourceType:$source")
  }

}