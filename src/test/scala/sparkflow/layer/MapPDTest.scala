package sparkflow.layer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import sparkflow.FlowFuncs._
import sparkflow.serialization.Formats.SerializedPD
import sparkflow.execute.Run.getRDD

/**
  * Created by ngoehausen on 2/29/16.
  */
class MapPDTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("basicMap"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 5)
    val doubled = filtered.map(_ * 2)

    val compactPd = doubled.toSerializedPD()
    val str = compactPd.toString()

    println(str)
    val recovered = SerializedPD.fromString(str)

    val rdd = getRDD(recovered, sc)
    Seq(2,4,6,8) should contain theSameElementsAs rdd.collect()
  }

}
