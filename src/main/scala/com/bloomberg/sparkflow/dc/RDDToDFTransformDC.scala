package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, Row}

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by ngoehausen on 5/11/16.
  */
class RDDToDFTransformDC[T <: Product : TypeTag](val prev: DC[T])extends DC[Row](Seq(prev)) {

  def computeSparkResults(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val df = prev.getRDD(sc).toDF()
    (df.rdd, Some(df.schema))
  }

  override def computeSignature() = {
    hashSeq(Seq(prev.getSignature, "toDF"))
  }

}
