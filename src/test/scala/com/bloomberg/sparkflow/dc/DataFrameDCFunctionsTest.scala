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
    val trashFires = parallelize(Seq(TrashFire(1,1)))
    val trashFires2 = parallelize(Seq(TrashFire(2,2)))

    val result = trashFires.union(trashFires2)
    val expected = Seq(Row(1,1), Row(2,2))

    expected should contain theSameElementsAs result.getDF(sc).collect()
  }

}
