package com.bloomberg.sparkflow.serialization

import java.security.MessageDigest

import com.google.common.io.BaseEncoding
import com.bloomberg.sparkflow.serialization.ClassExploration.{getClasses, getClassReader}


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

    val allDepedendentClasses = getClasses(obj).toList.sortBy(_.getName)
    val combinedHashTarget = allDepedendentClasses
      .map(getClassReader)
      .map(_.b)
      .map(hashBytes)
      .mkString("")

    hashString(combinedHashTarget)

  }

}
