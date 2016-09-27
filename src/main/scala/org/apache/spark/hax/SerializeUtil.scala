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

package org.apache.spark.hax

import java.nio.ByteBuffer

import com.google.common.io.BaseEncoding
import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.ClosureCleaner

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/1/16.
  */
object SerializeUtil {

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private def serialize[T: ClassTag](obj: T) = {
    serializer.serialize(obj)
  }

  private def deserialize[T: ClassTag](b: ByteBuffer) = {
    serializer.deserialize[T](b)
  }

  def objToString[T: ClassTag](obj: T): String = {
    BaseEncoding.base64().encode(serialize(obj).array())
  }

  def stringToObj[T: ClassTag](s: String): T = {
    deserialize(ByteBuffer.wrap(BaseEncoding.base64().decode(s)))
  }

  def clean(obj: AnyRef) = {
    ClosureCleaner.clean(obj)
  }

}
