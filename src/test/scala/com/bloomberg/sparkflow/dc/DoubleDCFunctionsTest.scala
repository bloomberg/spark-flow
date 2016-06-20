package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

/**
  * Created by rely10 on 6/20/16.
  */
class DoubleDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("sum") {
    val input = parallelize(Seq(1.0,1.0,2.0,3.0))
    val result = input.sum

    result.get(sc) should equal(7.0)
  }

  test("stats") {
    val input = parallelize(Seq(1.0,1.0,3.0,3.0))
    val result = input.stats

    result.get(sc).mean should equal(2.0)
  }

  test("mean") {
    val input = parallelize(Seq(1.0,1.0,3.0,3.0))
    val result = input.mean

    result.get(sc) should equal(2.0)
  }

  test("variance") {
    val input = parallelize(Seq(1.0,1.0,3.0,3.0))
    val result = input.variance

    result.get(sc) should equal(1.0)
  }

  test("stdev") {
    val input = parallelize(Seq(1.0,1.0,3.0,3.0))
    val result = input.stdev

    result.get(sc) should equal(1.0)
  }

}
