package com.bloomberg

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import com.bloomberg.sparkflow.dc.{SourceDC, ParallelCollectionDC, DC}

import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


import scala.language.implicitConversions

/**
  * Created by ngoehausen on 3/24/16.
  */
package object sparkflow extends SQLImplicits {

  private[sparkflow] def setSession(spark: SparkSession): Unit ={
    _spark = spark
    sqlContext = SQLContext.getOrCreate(spark.sparkContext)
  }

  private[sparkflow] def setSession(sc: SparkContext): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    setSession(spark)
  }

  private[sparkflow] def getSpark(sc: SparkContext): SparkSession ={
    synchronized {
      setSession(sc)
      spark
    }
  }

  private var _spark: SparkSession = null
  private var sqlContext: SQLContext = null

  protected override def _sqlContext: SQLContext = sqlContext
  private def spark: SparkSession = _spark

  implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
  implicit val denseVectorEncoder = org.apache.spark.sql.Encoders.kryo[DenseVector]
  implicit val sparseVectorEncoder = org.apache.spark.sql.Encoders.kryo[SparseVector]
  implicit val vectorEncoder = org.apache.spark.sql.Encoders.kryo[Vector]

  def read(implicit rowEncoder: Encoder[Row]) = new DCDataFrameReader

  def parallelize[T:ClassTag](seq: Seq[T])(implicit tEncoder: Encoder[T]): DC[T] = {
    new ParallelCollectionDC(seq)
  }

  def parallelize[T:ClassTag](seq: Seq[T], numSlices: Int)(implicit tEncoder: Encoder[T]): DC[T] = {
    new ParallelCollectionDC(seq, Some(numSlices))
  }

  def textFile(path: String) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path)
    new SourceDC[String](path, sourceFunc, "textFile")
  }

  def textFile(path: String,
               minPartitions: Int) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path, minPartitions)
    new SourceDC[String](path, sourceFunc, "textFile")
  }

  def objectFile[T:ClassTag](path: String)(implicit tEncoder: Encoder[T]) = {
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path)
    new SourceDC[T](path, sourceFunc, "objectFile")
  }

  def objectFile[T:ClassTag](path: String,
                             minPartitions: Int)(implicit tEncoder: Encoder[T]) = {
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    new SourceDC[T](path, sourceFunc, "objectFile")
  }

  private[sparkflow] var checkpointDir = "/tmp/sparkflow"
  def setCheckpointDir(s: String) = {checkpointDir = s}

  private[sparkflow] var defaultPersistence = StorageLevel.MEMORY_AND_DISK
  def setPersistence(storageLevel: StorageLevel) = {defaultPersistence = storageLevel}

}
