/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bloomberg.sparkflow.components

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow.dc.{DC, Dependency}
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.graphs._

import scala.reflect.ClassTag


/**
  * Created by ngoehausen on 3/3/16.
  */
class ComponentTest extends FunSuite with SharedSparkContext with ShouldMatchers{
/*
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
*/

  test("messiergraphs"){
    val a = parallelize(1 to 100)
    val b = a.filter(_ <= 50)
    val c = a.filter(_ > 50)
    val d = b.map(_ * 2)
    val e = c.union(d)


    val g = graphs.Util.buildDAG(a)

    println(a.getSignature +" " + b.getSignature+ " " + c.getSignature+" "+ d.getSignature+" "+e.getSignature)

    println(g.nodes.size)
    for(x <- g.nodes.toList){
      print(x.id + "  ")
    }

    println
    println(g.edges.size)
    for(x <- g.edges){
      print(x.src + "->" + x.dest +"  ")
    }

    println


/*
    var explored: Set[Dependency[_]] = Set()
    var toexplore: Set[Dependency[_]] = Set(c)
    val x = toexplore.head
    val toadd = (x.parents ++ x.children).toSet -- explored

    println((toexplore ++ toadd).getClass.getName)



    toexplore = toexplore ++ toadd
    println(toexplore.getClass.getName)
    println(toadd.getClass.getName)
    explored = explored + x
    toexplore = toexplore - x

    println(explored)
    println(toexplore)

*/
  }

}