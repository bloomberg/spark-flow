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

import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.util.StatCounter

/**
  * Created by ngoehausen on 4/19/16.
  */
class DoubleDCFunctions(self: DC[Double]) {

  def sum: DR[Double] = {
    self.mapToResult(_.sum)
  }

  def stats: DR[StatCounter] = {
    self.mapToResult(_.stats)
  }

  def mean: DR[Double] = {
    self.mapToResult(_.mean)
  }

  def variance: DR[Double] = {
    self.mapToResult(_.variance)
  }

  def stdev: DR[Double] = {
    self.mapToResult(_.stdev)
  }

  def sampleStdev: DR[Double] = {
    self.mapToResult(_.sampleStdev)
  }

  def sampleVariance: DR[Double] = {
    self.mapToResult(_.sampleVariance)
  }

  //  Experimental
  def meanApprox(timeout: Long,
                 confidence: Double = 0.95): DR[PartialResult[BoundedDouble]] = {
    self.mapToResult(_.meanApprox(timeout, confidence))
  }

  //  Experimental
  def sumApprox(timeout: Long,
                confidence: Double = 0.95): DR[PartialResult[BoundedDouble]] = {
    self.mapToResult(_.sumApprox(timeout, confidence))
  }

  def histogram(bucketCount: Int): DR[(Array[Double], Array[Long])] = {
    self.mapToResult(_.histogram(bucketCount))
  }

  def histogram(buckets: Array[Double], evenBuckets: Boolean = false): DR[Array[Long]] = {
    self.mapToResult(_.histogram(buckets, evenBuckets))
  }

}
