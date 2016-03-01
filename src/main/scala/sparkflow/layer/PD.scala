package sparkflow.layer

import sparkflow.serialization.Formats.CompactPD

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
abstract class PD[T: ClassTag](deps: Seq[PD[_]]) extends Serializable {

  def toCompactPD() : CompactPD

  def map[U: ClassTag](f: T => U): PD[U] = {
    new MapPD[U, T](this, f)
  }

}
