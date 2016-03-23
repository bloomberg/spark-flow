package sparkflow.serialization

import java.security.MessageDigest

import com.google.common.io.BaseEncoding
import org.objectweb.asm.ClassReader


/**
  * Created by ngoehausen on 3/23/16.
  */
private[sparkflow] object Hashing {

  def hashBytes(bytes: Array[Byte]) = {
    val md5 = MessageDigest.getInstance("MD5").digest(bytes)
    BaseEncoding.base64().encode(md5)
  }

  def hashString(s: String) = {
    hashBytes(s.getBytes())
  }

  def hashClass(obj: AnyRef) = {
    val className = obj.getClass.getName
    val resourceName = "/" + className.replace(".", "/") + ".class"
    val inputStream = obj.getClass.getResourceAsStream(resourceName)

    val reader = new ClassReader(inputStream)
    hashBytes(reader.b)
  }
}
