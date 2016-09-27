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
import org.apache.spark.sql._
import com.bloomberg.sparkflow.emptyRowEncoder

/**
  * Created by ngoehausen on 5/2/16.
  */
class MultiDatasetTransformDC(left: DC[_], right: DC[_],
                              f: (Dataset[_], Dataset[_]) => DataFrame, hashTarget: Seq[String]) extends DC[Row](emptyRowEncoder, Seq(left, right)) {

  def computeDataset(spark: SparkSession) = {
    val df = f(left.getDF(spark), right.getDF(spark))
    df
  }

  override def computeSignature() = {
    hashString(left.getSignature + right.getSignature + hashSeq(hashTarget))
  }

}
