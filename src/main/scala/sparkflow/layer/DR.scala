package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sparkflow.serialization.Hashing._

import scala.reflect.ClassTag

/**
  * Deferred Result
  */
class DR[T:ClassTag,U:ClassTag]
(val prev: DC[T],
 val f: RDD[T] => U) extends Dependency[U] {

  private var result: U = _

  def getResult(sc: SparkContext) = {
    if (result == null){
      result = computeResult(sc)
    }
    result
  }

  private def computeResult(sc: SparkContext) = {
    f(prev.getRDD(sc))
  }

  override def computeHash() = {
    hashString(prev.getHash + hashClass(f))
  }


}
