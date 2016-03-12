package sparkflow.layer

import org.apache.spark.hax.SerializeUtil._
import sparkflow.serialization.Formats._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/1/16.
  */
class FilterPD[T:ClassTag](val prev: PD[T], f: T => Boolean) extends PD[T](Seq(prev)) {

  def toSerializedPD() = {
    val transform = Transform(TransformType.Filter, objToString(f))
    SerializedPD(Seq(prev.toSerializedPD()), transform)
  }

}
