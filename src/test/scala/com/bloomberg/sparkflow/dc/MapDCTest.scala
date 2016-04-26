package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 2/29/16.
  */
class MapDCTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("basicMap"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 5)
    val doubled = filtered.map(_ * 2)


    val rdd = doubled.getRDD(sc)
    Seq(2,4,6,8) should contain theSameElementsAs rdd.collect()
  }



}
