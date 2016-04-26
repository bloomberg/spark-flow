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

    val classNames = ClassExploration.getClasses(g).map(_.getName)

    val expected = Set(
      "sparkflow.serialization.SomeFunctions$",
      "sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$3",
      "sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$4",
      "scala.Function1",
      "sparkflow.serialization.SomeFunctions$$anonfun$3",
      "sparkflow.serialization.ClassExplorationTest$$anonfun$1$$anonfun$2"
    )

    expected should contain theSameElementsAs classNames

  }



}
