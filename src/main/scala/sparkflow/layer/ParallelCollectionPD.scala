package sparkflow.layer

import org.apache.spark.hax.SerializeUtil._
import sparkflow.serialization.Formats.{CompactPD, TransformType, Transform}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
class ParallelCollectionPD[T:ClassTag](val data: Seq[T]) extends PD[T](Nil) {

  def toCompactPD() = {
    val transform = Transform(TransformType.Parallelize, objToString(data))
    CompactPD(Nil, transform)
  }

}
