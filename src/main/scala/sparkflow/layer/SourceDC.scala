package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import sparkflow.serialization.Hashing

/**
  * Created by ngoehausen on 3/23/16.
  */
class SourceDC[T: ClassTag](val source: String, val sourceFunc: SparkContext => RDD[T], val sourceType: String) extends DC[T](Nil)  {

  override def computeRDD(sc: SparkContext) = {
    sourceFunc(sc)
  }

  override def computeHash() = {
    Hashing.hashString(s"$sourceType:$source")
  }

}