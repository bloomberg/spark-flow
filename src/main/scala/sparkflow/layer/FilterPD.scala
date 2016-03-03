package sparkflow.layer

import org.apache.spark.hax.SerializeUtil._
import sparkflow.serialization.Formats.{CompactPD, TransformType, Transform}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 3/1/16.
  */
class FilterPD[T:ClassTag](val prev: PD[T], f: T => Boolean) extends PD[T](Seq(prev)) {

  def toCompactPD() = {
    val transform = Transform(TransformType.Filter, objToString(f))
    CompactPD(Seq(prev.toCompactPD()), transform)
  }

}
