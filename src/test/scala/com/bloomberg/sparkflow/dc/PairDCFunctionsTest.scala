package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 4/19/16.
  */
class PairDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers{


  test("reduceByKey"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.reduceByKey(_ + _)

    Seq((1,3), (2,7)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("reduceByKey(numPartitions)"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.reduceByKey(_ + _, 2)

    Seq((1,3), (2,7)) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("countApproxDistinctByKey"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.countApproxDistinctByKey(0.3)

    Seq((1,2), (2,2)) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("countApproxDistinctByKey(numPartitions)"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.countApproxDistinctByKey(0.3, 2)

    Seq((1,2), (2,2)) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("groupByKey"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.groupByKey()

    Seq((1, Seq(1,2)), (2, Seq(3,4))) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("groupByKey(numPartitions)"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.groupByKey(2)

    Seq((1, Seq(1,2)), (2, Seq(3,4))) should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("join"){
    val left = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val right = parallelize(Seq((1,"a"), (2,"b")))
    val result = left.join(right)

    val expected = Seq((1,(1, "a")), (1,(2,"a")), (2,(3,"b")), (2,(4,"b")))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("join(numPartitions)"){
    val left = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val right = parallelize(Seq((1,"a"), (2,"b")))
    val result = left.join(right, 2)

    val expected = Seq((1,(1, "a")), (1,(2,"a")), (2,(3,"b")), (2,(4,"b")))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("leftOuterJoin"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.leftOuterJoin(right)

    val expected = Seq((1,(1,Some("a"))), (1,(2,Some("a"))), (2,(3,None)))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("leftOuterJoin(numPartitions)"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.leftOuterJoin(right, 2)

    val expected = Seq((1,(1,Some("a"))), (1,(2,Some("a"))), (2,(3,None)))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("rightOuterJoin"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.rightOuterJoin(right)

    val expected = Seq((1,(Some(1),"a")), (1,(Some(2),"a")), (3,(None,"b")))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("rightOuterJoin(numPartitions)"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.rightOuterJoin(right, 2)

    val expected = Seq((1,(Some(1),"a")), (1,(Some(2),"a")), (3,(None,"b")))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("fullOuterJoin"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.fullOuterJoin(right)

    val expected = Seq((1,(Some(1),Some("a"))), (1,(Some(2),Some("a"))), (2,(Some(3),None)), (3,(None,Some("b"))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("fullOuterJoin(numPartitions)"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.fullOuterJoin(right, 2)

    val expected = Seq((1,(Some(1),Some("a"))), (1,(Some(2),Some("a"))), (2,(Some(3),None)), (3,(None,Some("b"))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("cogroup(other)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val result = first.cogroup(second)

    val expected = Seq((1, (Seq(1,2), Seq("a"))), (2, (Seq(3,4), Seq("b"))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("cogroup(other1,other2)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val result = first.cogroup(second, third)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'))), (2, (Seq(3,4), Seq("b"), Seq('d'))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("cogroup(other1,other2,other3)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val fourth = parallelize(Seq((1,true), (2,false)))
    val result = first.cogroup(second, third, fourth)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'), Seq(true))), (2, (Seq(3,4), Seq("b"), Seq('d'), Seq(false))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("cogroup(other,numPartitions)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val result = first.cogroup(second, 2)

    val expected = Seq((1, (Seq(1,2), Seq("a"))), (2, (Seq(3,4), Seq("b"))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("cogroup(other1,other2,numPartitions)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val result = first.cogroup(second, third, 2)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'))), (2, (Seq(3,4), Seq("b"), Seq('d'))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("cogroup(other1,other2,other3,numPartitions)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val fourth = parallelize(Seq((1,true), (2,false)))
    val result = first.cogroup(second, third, fourth, 2)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'), Seq(true))), (2, (Seq(3,4), Seq("b"), Seq('d'), Seq(false))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("groupWith(other)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val result = first.groupWith(second)

    val expected = Seq((1, (Seq(1,2), Seq("a"))), (2, (Seq(3,4), Seq("b"))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("groupWith(other1,other2)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val result = first.groupWith(second, third)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'))), (2, (Seq(3,4), Seq("b"), Seq('d'))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("groupWith(other1,other2,other3)"){
    val first = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val second = parallelize(Seq((1,"a"), (2,"b")))
    val third = parallelize(Seq((1,'c'), (2,'d')))
    val fourth = parallelize(Seq((1,true), (2,false)))
    val result = first.groupWith(second, third, fourth)

    val expected = Seq((1, (Seq(1,2), Seq("a"), Seq('c'), Seq(true))), (2, (Seq(3,4), Seq("b"), Seq('d'), Seq(false))))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("subtractByKey"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.subtractByKey(right)

    val expected = Seq((2,3))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("subtractByKey(numPartitions)"){
    val left = parallelize(Seq((1,1), (1,2), (2,3)))
    val right = parallelize(Seq((1,"a"), (3,"b")))
    val result = left.subtractByKey(right, 2)

    val expected = Seq((2,3))
    expected should contain theSameElementsAs result.getRDD(sc).collect()
    result.getRDD(sc).partitions.size shouldEqual 2
  }

  test("keys"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,4)))
    val result = input.keys

    Seq(1, 1, 2, 2) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("values"){
    val input = parallelize(Seq((1,1), (1,2), (2,3), (2,1)))
    val result = input.values

    Seq(1, 2, 3, 1) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("partitionBy"){
    val input = parallelize(Seq((2,3), (1,2), (1,1), (2,1)), 2)

    val partitioned = input.partitionByKey()

    val result = partitioned.mapPartitions(in => Iterator(in.toList))

    Seq(List((1,2), (1,1)), List((2,3), (2,1))) should contain theSameElementsAs result.getRDD(sc).collect()
  }

  test("keyBy"){
    val input = parallelize(Seq("dog", "fish", "horse"))
    val result = input.keyBy(_.size)

    Seq((3, "dog"), (4, "fish"), (5, "horse")) should contain theSameElementsAs result.getRDD(sc).collect()
  }

}
