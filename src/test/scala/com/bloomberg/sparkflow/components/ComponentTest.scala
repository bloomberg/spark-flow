package com.bloomberg.sparkflow.components

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import sparkflow.layer.DC
import sparkflow._


/**
  * Created by ngoehausen on 3/3/16.
  */
class ComponentTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("basicComponent"){

    case class InBundle(nums: DC[Int]) extends Bundle
    case class OutBundle(lt5: DC[Int], gt5: DC[Int]) extends Bundle

    val bundle = InBundle(parallelize(1 to 10))

    class TestComp(in: InBundle) extends Component[InBundle, OutBundle](in: InBundle){

      def run() = {
        val lt5 = in.nums.filter(_ < 5)
        val gt5 = in.nums.filter(_ > 5)
        OutBundle(lt5, gt5)
      }
    }

    val comp = new TestComp(bundle)

    1 to 4 should contain theSameElementsAs comp.output.lt5.getRDD(sc).collect()
    6 to 10 should contain theSameElementsAs comp.output.gt5.getRDD(sc).collect()

  }


}
