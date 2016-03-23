package sparkflow

import org.scalatest.FunSuite
import sparkflow.FlowFuncs._

/**
  * Created by ngoehausen on 3/23/16.
  */
class FlowFuncsTest extends FunSuite {

  test("hashing") {
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 5)
    val doubled = filtered.map(_ * 2)
    println(doubled.getHash)
    println(filtered.getHash)
  }
}
