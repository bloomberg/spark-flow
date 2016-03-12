package sparkflow.layer

import org.apache.spark.hax.SerializeUtil._
import org.apache.spark.rdd.RDD
import sparkflow.serialization.Formats._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/8/16.
  */
class RDDFuncPD[U:ClassTag, T:ClassTag](val prev: PD[T], f: RDD[T] => RDD[U]) extends PD[U](Seq(prev)) {

  def toSerializedPD() = {
    val transform = Transform(TransformType.RDDFunc, objToString(f))
    SerializedPD(Seq(prev.toSerializedPD()), transform)
  }

}
