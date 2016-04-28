package com.bloomberg.sparkflow.dc

import com.bloomberg.sparkflow.serialization.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, Row}

/**
  * Created by ngoehausen on 4/26/16.
  */
class DataframeSourceDC(f: SQLContext => DataFrame, path: String) extends DC[Row](Nil) {

  override def computeSparkResults(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val dataFrame = f(sqlContext)
    (dataFrame.rdd, Some(dataFrame.schema))
  }

  override def computeSignature() = {
    Hashing.hashString(Hashing.hashClass(f) + Hashing.hashString(path))
  }

}
