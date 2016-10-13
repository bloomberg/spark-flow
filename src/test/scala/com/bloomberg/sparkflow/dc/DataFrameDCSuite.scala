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
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Encoder}
import org.scalatest._
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.CaseClasses._

import scala.reflect.ClassTag
import scala.util.Random


/**
  * Created by ngoehausen on 4/26/16.
  */
class DataFrameDCSuite extends FunSuite with SharedSparkContext with ShouldMatchers {

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  test("csv"){
    val cars = read
      .format("csv")
      .option("header", "true")
      .load(testFile("cars.csv"))

    val makeModel = cars.select("make", "model")
    val make = cars.select("make").checkpoint()

    assert(make.getSignature != makeModel.getSignature)

    assert(make.getDF(sc).collect().nonEmpty)
  }

  test("json"){
    val path = "test.json"

    val dc = read.json(testFile(path)).repartition(10)

    val providerURLS = dc.select("provider_urls").checkpoint()

    assert(providerURLS.getDF(sc).columns.head == "provider_urls")

  }

  test("union"){
    val trashFires = parallelize(Seq(TrashFire(1,1)))
    val trashFires2 = parallelize(Seq(TrashFire(2,2)))

    val result = trashFires.union(trashFires2)
    val expected = Seq(Row(1,1), Row(2,2))

    expected should contain theSameElementsAs result.getDF(sc).collect()
  }

}
