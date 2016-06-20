package com.bloomberg.sparkflow.dc

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

}
