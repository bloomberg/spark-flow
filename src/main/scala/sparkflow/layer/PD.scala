package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sparkflow.serialization.Formats.SerializedPD

import scala.reflect.ClassTag

import sparkflow.execute.Run.getRDD

/**
  * Created by ngoehausen on 2/29/16.
  */
abstract class PD[T: ClassTag](deps: Seq[PD[_]]) extends Serializable {

  def toSerializedPD() : SerializedPD

  def map[U: ClassTag](f: T => U): PD[U] = {
    new MapPD[U, T](this, f)
  }

  def filter(f: T => Boolean): PD[T] = {
    new FilterPD[T](this, f)
  }

  def toRDD(sc: SparkContext): RDD[T] = {
    getRDD(this.toSerializedPD(), sc).asInstanceOf[RDD[T]]
  }

  def zipWithUniqueId(): PD[(T, Long)] = {
    new RDDFuncPD(this, (in: RDD[T]) => in.zipWithUniqueId())
  }

}
