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
import org.apache.spark.sql.{Encoder, SparkSession}


/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T](encoder: Encoder[T], val data: Seq[T], numSlices: Option[Int]) extends DC[T](encoder, Nil) {


  def computeDataset(spark: SparkSession) = {
    numSlices match {
      case Some(n) => spark.createDataset(data).repartition(n)
      case None => spark.createDataset(data)
    }
  }

  override def computeSignature() = {
    Hashing.hashString(data.map(_.toString).mkString("|"))
  }


}
