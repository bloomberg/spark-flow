package sparkflow.serialization

import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}

import java.security.MessageDigest

/**
  * Created by ngoehausen on 2/29/16.
  */
object Formats {

  implicit val formats = org.json4s.DefaultFormats +
    new EnumNameSerializer(TransformType)

  case class SerializedDC(parents: Seq[SerializedDC], transform: Transform){

    override def toString = write(this)

    def fromString(s: String) = read[SerializedDC](s)

    def computeHash(): String ={
      hashString(parents.map(_.computeHash()).mkString("") + transform.encodedTransform)
    }

    private def hashString(s: String) = {
      new String(MessageDigest.getInstance("MD5").digest(s.getBytes))
    }
  }

  case class Transform(transformType: TransformType.Value, encodedTransform: String)

  object TransformType extends Enumeration{
    val Parallelize, Map, Filter, FlatMap, RDDFunc = Value
  }

  object SerializedDC{
    def fromString(s: String) = read[SerializedDC](s)
  }


}
