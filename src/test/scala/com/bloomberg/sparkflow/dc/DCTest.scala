package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import sparkflow._

/**
  * Created by ngoehausen on 3/24/16.
  */
class DCTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("checkpointing"){
    val another = (x: Int) => x < 4
    val filterFunc = (x: Int) => another(x)

    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 3).checkpoint()
    val doubled = filtered.map(_ * 2)


    val rdd = doubled.getRDD(sc)
    rdd.foreach(println)
  }

}
