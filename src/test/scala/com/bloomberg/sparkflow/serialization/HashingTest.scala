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
    var param = 7
    val input = 5

    val another = (x: Int) => x * 2
    val nested = (x: Int) => x * 4 + param + another(x)
    val g = (x: Int) => nested(x) + param

    val initialOutput = g(input)
    val initialGHash = hashClass(g)
    assert(initialGHash != hashClass(nested))
    assert(initialGHash != hashClass(another))

    assert(initialGHash == hashClass(g))
    param = 10
    assert(initialGHash != hashClass(g))
    assert(initialOutput != g(input))

  }

  test("dcHashing"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 6)
    val doubled = filtered.map(_ * 2)
    val after = doubled.map(SomeFunctions.func4)

    println(numbers.getSignature)
    println(filtered.getSignature)
    println(doubled.getSignature)
    println(after.getSignature)


  }
}
