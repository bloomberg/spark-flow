package com.bloomberg.sparkflow.dc


import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by ngoehausen on 5/18/16.
  */
object Util {


  private[dc] def saveCheckpoint[T:ClassTag](checkpointPath: String, rdd: RDD[T], schema: Option[StructType], isDataFrame: Boolean) = {
    assert(rdd != null)
    val sc = rdd.context
    if (schema.isDefined && isDataFrame){
      val sqlContext = SQLContext.getOrCreate(sc)
      val rowRDD = rdd.asInstanceOf[RDD[Row]]
      sqlContext.createDataFrame(rowRDD, schema.get).write.mode(SaveMode.Overwrite).parquet(checkpointPath)
    } else {
      if (pathExists(checkpointPath, sc)){
        deletePath(checkpointPath, sc)
      }
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
    if (pathExists(checkpointPath, sc)) {
      val sqlContext = SQLContext.getOrCreate(sc)
      Try {
        val df = sqlContext.read.parquet(checkpointPath)
        df.count()
        df
      }.toOption
    } else {
      None
    }
  }

  private def attemptRDDLoad[T: ClassTag](checkpointPath: String, sc: SparkContext): Option[RDD[T]] = {
    val path = new Path(checkpointPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      Try{
        val rdd = sc.objectFile[T](checkpointPath)
        rdd.count()
        rdd
      }.toOption
    } else {
      None
    }

  }

  def pathExists(dir: String, sc: SparkContext) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.exists(path)
  }

  def deletePath(dir: String, sc: SparkContext) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.delete(path, true)
  }

}
