/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bloomberg.sparkflow.serialization

import org.scalatest._
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.serialization.HashingSample
import com.bloomberg.sparkflow.serialization.ClassExploration._
import com.bloomberg.sparkflow.serialization.Hashing._
import com.holdenkarau.spark.testing.SharedSparkContext

/**
  * Created by ngoehausen on 3/23/16.
  */
class HashingTest extends FunSuite with SharedSparkContext with ShouldMatchers{

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

    val allSignatures = Set(numbers.getSignature,
      filtered.getSignature,
      doubled.getSignature,
      after.getSignature)

    assert(allSignatures.size == 4)
  }

  test("caseHashing"){
//    println(s"fieldObjects: ${getFieldObjects(HashingSample.result)}")
//    println(s"result: ${HashingSample.result.getSignature}")
   
   assert(HashingSample.result.getSignature.length > 0)
  }


}



