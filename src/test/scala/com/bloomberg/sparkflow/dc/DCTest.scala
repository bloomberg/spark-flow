package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 3/24/16.
  */
class DCTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("checkpointing"){
    val another = (x: Int) => x < 4
    val filterFunc = (x: Int) => another(x)

    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 3).checkpoint()
    val doubled = filtered.map(_ * 2)


    val rdd = doubled.getRDD(sc)
    rdd.foreach(println)
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
    val input = parallelize(Seq(1 to 100), 3)
    val result = input.coalesce(2)

    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("coalesceBigger"){
    val input = parallelize(Seq(1 to 100), 3)
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

  test("zip"){
    val left = parallelize(Seq(1,2,3))
    val right = left.map(x => x * 2)
    val result = left.zip(right)

    Seq((1,2), (2,4), (3,6)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

}
