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
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 6/29/16.
  */
class DatasetDCSuite extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("encoders"){

    val dc = parallelize(1 to 3).map(x => (x, Seq(x)))
    val rdd = dc.getRDD(sc)
    val expected = (1 to 3).map(x => (x, Seq(x)))
    rdd.collect() should contain theSameElementsAs expected
  }

}
