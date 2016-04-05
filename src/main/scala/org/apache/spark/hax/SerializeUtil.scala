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

  private def serialize[T:ClassTag](obj: T) ={
    serializer.serialize(obj)
  }

  private def deserialize[T:ClassTag](b: ByteBuffer) ={
    serializer.deserialize[T](b)
  }

  def objToString[T:ClassTag](obj: T): String = {
    BaseEncoding.base64().encode(serialize(obj).array())
  }

  def stringToObj[T:ClassTag](s: String): T = {
    deserialize(ByteBuffer.wrap(BaseEncoding.base64().decode(s)))
  }

  def clean(obj: AnyRef) = {
    ClosureCleaner.clean(obj)
  }

}
