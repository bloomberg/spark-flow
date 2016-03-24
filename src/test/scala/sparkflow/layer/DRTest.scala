package sparkflow.layer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import sparkflow.FlowFuncs._

/**
  * Created by ngoehausen on 3/23/16.
  */
class DRTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("normalize"){
    val numbers = parallelize(1 to 10)
    val doubles = numbers.map(_.toDouble)
    val sum = doubles.mapToResult(_.sum())
    val normalized = doubles.mapWith(sum)(_ / _)

    val rdd = normalized.getRDD(sc)
    rdd.foreach(println)
  }
}
