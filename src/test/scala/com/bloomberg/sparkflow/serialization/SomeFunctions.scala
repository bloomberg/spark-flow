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

package com.bloomberg.sparkflow.serialization

/**
  * Created by ngoehausen on 4/18/16.
  */

object SomeFunctions {

  val func1 = (x: Int) => x + 3

  val func2 = (x: Int) => x * 2

  val func3 = (x: Int) => x / 4

  def staticMethod(x: Int) = 205 + func3(x) + otherMethod(x)

  def otherMethod(x: Int) = x * 3

  val func4 = (x: Int) => 1 + staticMethod(x) + func2(x)

  val f5 = (x: Int) => (y: Int) =>  x * y

  val f6 = f5(6)

  val f7 = f5(7)

}
