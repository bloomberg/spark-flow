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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by ngoehausen on 4/19/16.
  */
class DRImpl[T, U](prev: DC[T], f: RDD[T] => U) extends DR[U](prev) {

  private var result: U = _

  override def get(spark: SparkSession) = {
    if (result == null) {
      result = computeResult(spark)
    }
    result
  }

  private def computeResult(spark: SparkSession) = {
    f(prev.getRDD(spark))
  }

  override def computeSignature() = {
    hashString(prev.getSignature + hashClass(f))
  }

}
