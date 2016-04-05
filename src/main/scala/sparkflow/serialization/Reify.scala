package sparkflow.serialization

import scala.reflect.runtime.{universe=>u}

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.{currentMirror => m}
/**
  *
  * Created by ngoehausen on 4/5/16.
  */
object Reify {

  val f = (x: Int) => x * 2
  val g = (x: Int) => f(x) + 2

  def main(args: Array[String]) {

    val expr = u reify(g)

    println(u show expr.tree)

    println()


    val tb = m.mkToolBox()

    val x = tb.compile(expr.tree)

    tb.frontEnd

    println(x.toString())


  }

}
