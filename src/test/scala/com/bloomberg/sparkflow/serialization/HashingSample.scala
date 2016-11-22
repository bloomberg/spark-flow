package com.bloomberg.sparkflow.serialization

import com.bloomberg.sparkflow._

/**
  * Created by rely10 on 11/18/16.
  */
object HashingSample {
  val result = Input.input.groupByKey()
    .map{case (key, values) => Func.playWithValues(values)}
}

object Input { // changes affect result
  val input = parallelize(Seq((1, 1), (1, 2), (2, 3), (2, 4)))
}

object Str { // TODO: changes should affect result
  val str = "moose"
}

object Func { // TODO: changes should affect result
  val playWithValues = (values: Seq[Int]) => {
    val newValues = values.map(v => Str.str)
    newValues
  }
}