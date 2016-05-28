package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.CaseClasses.TrashFire
import com.bloomberg.sparkflow._
import org.apache.spark.sql.Row
import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

/**
  * Created by rely10 on 5/27/16.
  */

class DataFrameDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("unionAll") {
    val trashFires = parallelize(Seq(TrashFire(1,1))).toDF()
    val trashFires2 = parallelize(Seq(TrashFire(2,2))).toDF()

    val result = trashFires.unionAll(trashFires2)
    val expected = Seq(Row(1,1), Row(2,2))

    expected should contain theSameElementsAs result.getDF(sc).collect()
  }

}
