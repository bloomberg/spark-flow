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

/**
  * Created by ngoehausen on 3/23/16.
  */
abstract class Dependency[T](val parents: Seq[Dependency[_]]) extends Serializable {

  val children = scala.collection.mutable.Set[Dependency[_]]()
  parents.map(_.children.add(this))

  //  val ct = classTag[T]
  protected def computeSignature(): String

  private var signature: String = _


  def getSignature: String = {
    if (signature == null) {
      this.signature = this.computeSignature()
    }
    signature
  }

}
