package sparkflow.serialization

import org.scalatest.FunSuite
import sparkflow._
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

  test("dcHashing"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 6)
    val doubled = filtered.map(_ * 2)

    println(numbers.getHash)
    println(filtered.getHash)
    println(doubled.getHash)


  }
}
