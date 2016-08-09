package com.bloomberg.sparkflow.components

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow.dc.DC
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.graphs._


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


  test("graphs"){
    val another = (x: Int) => x < 4
    val filterFunc = (x: Int) => another(x)

    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 3).checkpoint()
    val doubled = filtered.map(_ * 2)


    println(numbers.getSignature)
    println(filtered.getSignature)
    println(doubled.getSignature)
    println(graphs.Util.getGrandparents(doubled))

    val g = graphs.Util.buildDAG(numbers)
    val nodeList = g.nodes.values.toList
    println(nodeList.last.id)
    println(g.edges.last.src)


  }


  test("messiergraphs"){
    val a = parallelize(1 to 100)
    val b = a.filter(_ <= 50)
    val c = a.filter(_ > 50)
    val d = b.map(_ * 2)
    val e = c.union(d)

    val g = graphs.Util.buildDAG(a)

    println(a.getSignature +" " + b.getSignature+ " " + c.getSignature+" "+ d.getSignature+" "+e.getSignature)

    println(g.nodes.size)
    for(x <- g.nodes.values.toList){
      print(x.id + "  ")
    }

    println
    println(g.edges.size)
    for(x <- g.edges){
      print(x.src + "->" + x.dest +"  ")
    }

    println

  }

}