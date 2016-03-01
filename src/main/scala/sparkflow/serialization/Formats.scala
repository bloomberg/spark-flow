package sparkflow.serialization

import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}

/**
  * Created by ngoehausen on 2/29/16.
  */
object Formats {

  implicit val formats = org.json4s.DefaultFormats +
    new EnumNameSerializer(TransformType)

  case class CompactPD(parents: Seq[CompactPD], transform: Transform){

    override
    def toString = write(this)

    def fromString(s: String) = read[CompactPD](s)
  }

  case class Transform(transformType: TransformType.Value, encodedTransform: String)

  object TransformType extends Enumeration{
    val Map, Parallelize = Value
  }

  object CompactPD{
    def fromString(s: String) = read[CompactPD](s)
  }


}
