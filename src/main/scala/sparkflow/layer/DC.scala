package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T: ClassTag](deps: Seq[DC[_]]) extends Serializable {

  val ct = classTag[T]
  private var rdd: RDD[T] = _
  private var hash: String = _

  protected def computeRDD(sc: SparkContext): RDD[T]
  protected def computeHash(): String

  def getRDD(sc: SparkContext): RDD[T] = {
    if(rdd == null){
      this.rdd = this.computeRDD(sc)
    }
    rdd
  }

  def getHash: String = {
    if(hash == null){
      this.hash = this.computeHash()
    }
    hash
  }

  def map[U: ClassTag](f: T => U): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.map(f), f)
  }

  def filter(f: T => Boolean): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.filter(f), f)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.flatMap(f), f)
  }

  def zipWithUniqueId(): DC[(T, Long)] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.zipWithUniqueId, "zipWithUniqueId")
  }

}
