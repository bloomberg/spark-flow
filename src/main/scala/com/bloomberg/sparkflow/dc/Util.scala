package com.bloomberg.sparkflow.dc


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by ngoehausen on 5/18/16.
  */
object Util {


  private[dc] def saveCheckpoint[T:ClassTag](checkpointPath: String, rdd: RDD[T], schema: Option[StructType], isDataFrame: Boolean) = {
    assert(rdd != null)
    if (schema.isDefined && isDataFrame){
      val sqlContext = SQLContext.getOrCreate(rdd.context)
      val rowRDD = rdd.asInstanceOf[RDD[Row]]
      sqlContext.createDataFrame(rowRDD, schema.get).write.parquet(checkpointPath)
    } else {
      rdd.saveAsObjectFile(checkpointPath)
    }

  }

  private[dc] def loadCheckpoint[T: ClassTag](checkpointPath: String, sc: SparkContext, isDataFrame: Boolean): Option[(RDD[T], Option[StructType])] = {
    if (isDataFrame){
      val dfAttempt = attemptDFLoad(checkpointPath, sc)
      dfAttempt match {
        case Some(df) => Some((df.rdd.asInstanceOf[RDD[T]], Some(df.schema)))
        case None => None
      }
    } else{
      val rddAttempt = attemptRDDLoad[T](checkpointPath, sc)
      rddAttempt match {
        case Some(resultRDD) => Some((resultRDD, None))
        case None => None
      }
    }

  }

  private def attemptDFLoad(checkpointPath: String,sc: SparkContext): Option[DataFrame] = {
    Try{
      val sqlContext = SQLContext.getOrCreate(sc)
      val attemptDF = sqlContext.read.parquet(checkpointPath)
      attemptDF.first()
      attemptDF
    }.toOption
  }

  private def attemptRDDLoad[T: ClassTag](checkpointPath: String, sc: SparkContext): Option[RDD[T]] = {
    Try{
      val attemptRDD = sc.objectFile[T](checkpointPath)
      attemptRDD.first()
      attemptRDD
    }.toOption
  }
}
