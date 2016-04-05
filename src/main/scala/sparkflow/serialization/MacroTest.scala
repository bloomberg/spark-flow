package sparkflow.serialization

import scala.language.experimental.macros

/**
  * Created by ngoehausen on 4/4/16.
  */

import scala.language.experimental.macros

object MacroTest {

  def findFreeVariables(func: => Any):List[(String, Any)] = macro FindFreeVars.findMacro

  def invokeAndReturn(func: => Any, vals: List[(String, Any)]): List[(String, Any)] = {
    func
    vals
  }

  def execAndReturn[T, V](param: T)(func: (T) => V): V = macro FindFreeVars.execMacro[T, V]

  def execAndReturn[T, V](func: (T) => V, param: T, vars: List[(String, Any)]): V = {
    println(vars)
    func(param)
  }

  def snip1() = {
    val x = 2
    val fun = (x:Int) => (x + 2)
    println(
      findFreeVariables{
        val y = 3
        fun(x + y)
      }
    )
  }


//  def main(args: Array[String]) {
//    val nested = (x: Int) => x * 2
//    val f = (x: Int) => nested(x) + 5
//    println(
//      invokeAndReturn{
//        f
//      }
//    )
//  }

}
