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
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 2/29/16.
  */
class MapDCTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("basicMap"){
    val numbers = parallelize(1 to 10)
    val filtered = numbers.filter(_ < 5)
    val doubled = filtered.map(_ * 2)


    val rdd = doubled.getRDD(sc)
    Seq(2,4,6,8) should contain theSameElementsAs rdd.collect()
  }



}
