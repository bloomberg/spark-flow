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

package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types.{StructField, DataTypes, IntegerType, StructType}
import org.apache.spark.sql.{Row, SQLContext, DataFrame, Dataset}
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 3/24/16.
  */
class DCTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("map"){
    val input = parallelize(Seq(1,1,2,3))
    val result = input.map(_ + 3)

    Seq(4,4,5,6) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("checkpointing"){
    val another = (x: Int) => x < 4
    val filterFunc = (x: Int) => another(x)

    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 3).checkpoint()
    val doubled = filtered.map(_ * 2)

    Seq(2,4) should contain theSameElementsAs doubled.getRDD(sc).collect()
  }

  test("sample"){
    val input = parallelize(Seq(1,1,2,3))
    val result = input.sample(false, 0.75, 20L)

    result.getRDD(sc).collect() should contain(1)
    result.getRDD(sc).count should equal(3.0)
  }

  test("union"){
    val first = parallelize(Seq(1,2))
    val second = parallelize(Seq(3,4))
    val result = first.union(second)

    Seq(1,2,3,4) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("++"){
    val first = parallelize(Seq(1,2))
    val second = parallelize(Seq(3,4))
    val result = first.++(second)

    Seq(1,2,3,4) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("distinct"){
    val input = parallelize(Seq(1,1,2,3))
    val result = input.distinct()

    Seq(1,2,3) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("distinct(numPartitions)"){
    val input = parallelize(Seq(1,1,2,3))
    val result = input.distinct(2)

    Seq(1,2,3) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("coalesceSmaller"){
    val input = parallelize(1 to 100, 3)
    val result = input.coalesce(2)

    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("coalesceBigger"){
    val input = parallelize(1 to 100, 3)
    val result = input.coalesce(10, true)

    result.getRDD(sc).partitions.size shouldEqual 10
  }

  test("intersection"){
    val left = parallelize(Seq(1,2,3))
    val right = parallelize(Seq(2,3,4))
    val result = left.intersection(right)

    Seq(2,3) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("intersection(numPartitions)"){
    val left = parallelize(Seq(1,2,3))
    val right = parallelize(Seq(2,3,4))
    val result = left.intersection(right, 2)

    Seq(2,3) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("glom"){
    val input = parallelize(Seq(1,1,2), 1)
    val result = input.glom()

    Seq(Array(1,1,2)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("cartesian"){
    val left = parallelize(Seq(1,2))
    val right = parallelize(Seq(3,4))
    val result = left.cartesian(right)

    Seq((1,3), (1,4), (2,3), (2,4)) should contain theSameElementsAs result.getRDD(sc).collect()
  }
// //TODO switch to KeyValueGroupedDC
//  test("groupBy"){
//    val input = parallelize(1 to 5)
//    val result = input.groupBy(x => x % 3)
//
//    Seq((0, List(3)), (1, List(1,4)), (2, List(2,5))) should contain theSameElementsAs result.getRDD(sc).map(p => (p._1, p._2.toList)).collect()
//  }
//
//  test("groupBy(numPartitions)"){
//    val input = parallelize(1 to 5)
//    val result = input.groupBy(x => x % 3, 2)
//
//    Seq((0, List(3)), (1, List(1,4)), (2, List(2,5))) should contain theSameElementsAs result.getRDD(sc).map(p => (p._1, p._2.toList)).collect()
//    result.getRDD(sc).partitions.size shouldEqual 2
//  }

  test("zip"){
    val left = parallelize(Seq(1,2,3))
    val right = left.map(x => x * 2)
    val result = left.zip(right)

    Seq((1,2), (2,4), (3,6)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("zipWithIndex"){
    val input = parallelize(1 to 5)
    val result = input.zipWithIndex

    Seq((1, 0.0), (2, 1.0), (3, 2.0), (4, 3.0), (5, 4.0)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("subtract"){
    val left = parallelize(Seq(1,2,3,3))
    val right = parallelize(Seq(1,3))
    val result = left.subtract(right)

    Seq(2) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("subtract(numPartitions)"){
    val left = parallelize(Seq(1,2,3,3))
    val right = parallelize(Seq(1,3))
    val result = left.subtract(right, 2)

    Seq(2) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("sliding"){
    val input = parallelize(1 to 4)
    val result = input.sliding(2)

    Seq(Array(1,2), Array(2,3), Array(3,4)) should contain theSameElementsInOrderAs result.getRDD(sc).collect()

  }

  test("sortBy"){
    val input = parallelize(Seq(5, 2, 3, 1))
    val ascending = input.sortBy(x => x)
    val descending = input.sortBy(x => x, ascending = false)
    val ascending2Part = input.sortBy(x => x, ascending = true, 2)

    Seq(1, 2, 3, 5) should contain theSameElementsInOrderAs ascending.getRDD(sc).collect()
    Seq(5, 3, 2, 1) should contain theSameElementsInOrderAs descending.getRDD(sc).collect()
    ascending2Part.getRDD(sc).partitions.size shouldEqual 2

  }

//  Actions
//
  test("collect"){
    val input = parallelize(1 to 5)
    val result = input.collect

    result.get(sc) should equal(Array(1,2,3,4,5))
  }

  test("reduce"){
    val input = parallelize(1 to 5)
    val result = input.reduce(_ + _)

    result.get(sc) should equal(15)
  }

  test("fold"){
    val input = parallelize(1 to 5)
    val result = input.fold(0)(_ + _)

    result.get(sc) should equal(15)
  }

  test("count"){
    val input = parallelize(1 to 5)
    val result = input.count

    result.get(sc) should equal(5.0)
  }

  test("countByValue"){
    val input = parallelize(Seq(1,1,2,3))
    val result = input.countByValue

    Map((1,2.0), (2,1.0), (3,1.0)) should contain theSameElementsAs result.get(sc)
  }

  test("first"){
    val input = parallelize(Seq("first", "second"))
    val result = input.first

    result.get(sc) should equal("first")
  }

  test("take"){
    val input = parallelize(Seq("first", "second", "third"))
    val result = input.take(2)

    Seq("first", "second") should contain theSameElementsInOrderAs result.get(sc)
  }

  test("top"){
    val input = parallelize(Seq(1,5,2,4,3))
    val result = input.top(2)

    result.get(sc) should equal(Array(5,4))
  }

  test("takeOrdered"){
    val input = parallelize(Seq(1,5,2,4,3))
    val result = input.takeOrdered(2)

    result.get(sc) should equal(Array(1,2))
  }

  test("max"){
    val input = parallelize(Seq(1,5,2,4,3))
    val result = input.max

    result.get(sc) should equal(5)
  }

  test("min"){
    val input = parallelize(Seq(1,5,2,4,3))
    val result = input.min

    result.get(sc) should equal(1)
  }

  test("isEmpty"){
    val input1 = parallelize(Seq(1,2))
    val result1 = input1.isEmpty

    result1.get(sc) should equal(false)

    val input2 = parallelize(Seq[Int]())
    val result2 = input2.isEmpty

    result2.get(sc) should equal(true)
  }

}
