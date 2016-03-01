package sparkflow.layer

import sparkflow.serialization.Formats._

import scala.reflect.ClassTag
import org.apache.spark.hax.SerializeUtil._

/**
  * Created by ngoehausen on 2/29/16.
  */
class MapPD[U:ClassTag, T:ClassTag](val prev: PD[T], f: T => U) extends PD[U](Seq(prev)) {

  def toCompactPD() = {
    val transform = Transform(TransformType.Map, objToString(f))
    CompactPD(Seq(prev.toCompactPD()), transform)
  }

}
