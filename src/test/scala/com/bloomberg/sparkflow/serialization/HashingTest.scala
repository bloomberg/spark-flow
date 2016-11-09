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
import com.bloomberg.sparkflow.dc.DC
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
    setLogLevel("DEBUG")
    val numbers: DC[Int] = parallelize(1 to 10)

    var multiplier = 10
    val result1 = numbers.map({case x: Int if x % 3 == 0 => x * multiplier
                               case x: Int => x})

    Seq(1,2,30,4,5,60,7,8,90,10) should contain theSameElementsAs result1.getRDD(sc).collect()


    multiplier = 20
    val result2 = numbers.map({case x: Int if x % 3 == 0 => x * multiplier
                               case x: Int => x})

    Seq(1,2,60,4,5,120,7,8,180,10) should contain theSameElementsAs result2.getRDD(sc).collect()

    //var nest = (a: Int, b: Int) => (a, b * 2)
    //val result1 = input.map({case (a,b) => (a, multiply(b))})
    //val result2 = input.map{case (x: Int, y: Int) => nest}
  }
}



