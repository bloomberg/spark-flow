package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 6/29/16.
  */
class DatasetDCSuite extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("encoders"){

    parallelize(1 to 3).map(x => (x, Seq(x))).getRDD(sc).foreach(println)




  }

}
