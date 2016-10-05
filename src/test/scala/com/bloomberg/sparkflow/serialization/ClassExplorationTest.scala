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

import org.scalatest.{ShouldMatchers, FunSuite}

/**
  * Created by ngoehausen on 4/18/16.
  */
class ClassExplorationTest extends FunSuite with ShouldMatchers {


  test("basic class explore"){
    val evenMoreNested = (x: Int) => x + 7
    val nested = (x: Int) => evenMoreNested(x) + 5
    val g = (x: Int) =>  3 + nested(x) + SomeFunctions.staticMethod(x)

    val classNames = ClassExploration.getClassesAndSerializedFields(g)._1.map(_.getName)

    val expected = Set(
      "com.bloomberg.sparkflow.serialization.SomeFunctions$",
      "com.bloomberg.sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$3",
      "com.bloomberg.sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$4",
      "scala.Function1",
      "com.bloomberg.sparkflow.serialization.SomeFunctions$$anonfun$3",
      "com.bloomberg.sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$2"
    )

    expected should contain theSameElementsAs classNames

  }



}
