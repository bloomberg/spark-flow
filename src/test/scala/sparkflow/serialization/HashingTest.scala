package sparkflow.serialization

import org.scalatest.FunSuite
import sparkflow.serialization.Hashing._

/**
  * Created by ngoehausen on 3/23/16.
  */
class HashingTest extends FunSuite {

  test("functionHashing"){
    val f = (x: Int) => x * 4
    val g = (x: Int) => f(x)

    println(hashClass(f))
    println(hashClass(g))
  }
}
