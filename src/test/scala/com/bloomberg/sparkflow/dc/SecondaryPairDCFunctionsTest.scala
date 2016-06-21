package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{ShouldMatchers, FunSuite}

/**
  * Created by ngoehausen on 6/6/16.
  */
class SecondaryPairDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers {

//  test("testRepartAndSort") {
//    val input = parallelize(Seq(
//      (("a",3), 0),
//      (("b",2), 0),
//      (("b",1), 0),
//      (("b",3), 0),
//      (("a",2), 0),
//      (("a",1), 0)))
//
//    val sortAndRepart = input.repartitionAndSecondarySortWithinPartitions(2)
//
//    val result = sortAndRepart.mapPartitions(it => Iterator(it.toList))
//
//    val expected = Seq(
//      List(
//      (("a",1), 0),
//      (("a",2), 0),
//      (("a",3), 0)),
//      List(
//      (("b",1), 0),
//      (("b",2), 0),
//      (("b",3), 0)))
//
//    expected should contain theSameElementsAs result.getRDD(sc).collect()
//
//  }

}
