package com.bloomberg.sparkflow.serialization

import org.apache.spark.hax.MyClosureCleaner
import org.scalatest.FunSuite
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.hax.SerializeUtil.clean

/**
  * Created by ngoehausen on 3/23/16.
  */
class HashingTest extends FunSuite {

  test("functionHashing"){
    val const = 7

    val another = (x: Int) => x * 2

    val nested = (x: Int) => x * 4 + const + another(x)
    val g = (x: Int) => nested(x) + const

    println(hashClass(nested))
    println(hashClass(g))

    MyClosureCleaner.clean(g)
//    println(hashClass(clean(f)))
  }

  test("dcHashing"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 6)
    val doubled = filtered.map(_ * 2)
    val after = doubled.map(SomeFunctions.func4)

    println(numbers.getHash)
    println(filtered.getHash)
    println(doubled.getHash)
    println(after.getHash)


  }
}
