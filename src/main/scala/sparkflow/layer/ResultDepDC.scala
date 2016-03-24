package sparkflow.layer

import org.apache.spark.SparkContext
import sparkflow.serialization.Hashing._

import scala.reflect.ClassTag

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U:ClassTag, T:ClassTag, V: ClassTag]
(val prev: DC[T], dr: DR[_,U],f: (T,U) => V) extends DC[V](Seq(prev, dr)) {

  override def computeRDD(sc: SparkContext) = {
    val result = dr.getResult(sc)
    prev.getRDD(sc).mapPartitions(iterator => {
      iterator.map(t => f(t, result))
    })
  }

  override def computeHash() = {
    hashString(prev.getHash + dr.getHash + hashClass(f))
  }

}
