package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import sparkflow._

/**
  * Created by ngoehausen on 4/19/16.
  */
class PairDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers{


  test("reduceByKey"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.reduceByKey(_ + _)

    Seq((1,3), (2,7)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("join"){
    val left = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val right = parallelize(Seq((1,"a"), (2,"b")))
    val result = left.join(right)

    val expected = Seq((1,(1, "a")), (1,(2,"a")), (2,(3,"b")), (2,(4,"b")))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

}
