package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
  * Created by ngoehausen on 8/9/16.
  */
object EncoderUtil {

  def encoderFor[A : Encoder]: ExpressionEncoder[A]  = encoders.encoderFor

}
