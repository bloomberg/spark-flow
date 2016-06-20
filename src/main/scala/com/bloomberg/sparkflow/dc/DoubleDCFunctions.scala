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

  def histogram(bucketCount: Int): DR[Pair[Array[Double], Array[Long]]] = {
    self.mapToResult(_.histogram(bucketCount))
  }

  def histogram(buckets: Array[Double], evenBuckets: Boolean = false): DR[Array[Long]] = {
    self.mapToResult(_.histogram(buckets, evenBuckets))
  }

}
