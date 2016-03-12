package sparkflow.serialization

import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}

/**
  * Created by ngoehausen on 2/29/16.
  */
object Formats {

  implicit val formats = org.json4s.DefaultFormats +
    new EnumNameSerializer(TransformType)

  case class SerializedPD(parents: Seq[SerializedPD], transform: Transform){

    override def toString = write(this)

    def fromString(s: String) = read[SerializedPD](s)
  }

  case class Transform(transformType: TransformType.Value, encodedTransform: String)

  object TransformType extends Enumeration{
    val Parallelize, Map, Filter, FlatMap, RDDFunc = Value
  }

  object SerializedPD{
    def fromString(s: String) = read[SerializedPD](s)
  }


}
