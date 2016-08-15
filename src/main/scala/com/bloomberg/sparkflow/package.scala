package com.bloomberg

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import com.bloomberg.sparkflow.dc.{SourceDC, ParallelCollectionDC, DC}
import org.apache.spark.sql.catalyst.encoders.{RowEncoder, ExpressionEncoder}
import org.apache.spark.sql.types.StructType

import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.spark.sql.EncoderStuff.encFor

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

//  implicit def rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
//  implicit def rowEncoder: Encoder[Row] = ExpressionEncoder()
//  org.apache.spark.sql.Encoders
  implicit def denseVectorEncoder = org.apache.spark.sql.Encoders.kryo[DenseVector]
  implicit def sparseVectorEncoder = org.apache.spark.sql.Encoders.kryo[SparseVector]
  implicit def vectorEncoder = org.apache.spark.sql.Encoders.kryo[Vector]

  implicit def rowEnc(row: Row): Encoder[Row] = new IntRow(row)


  class IntRow(row: Row) extends Encoder[Row]{
    /** Returns the schema of encoding this type of object as a Row. */
    def schema: StructType = row.schema

    /** A ClassTag that can be used to construct and Array to contain a collection of `T`. */
    def clsTag: ClassTag[Row] = classTag[Row]
  }

  def read = new DCDataFrameReader

  def parallelize[T:Encoder](seq: Seq[T]): DC[T] = {
    val encoder = encFor[T]
    new ParallelCollectionDC(encoder, seq, None)
  }

  def parallelize[T:Encoder](seq: Seq[T], numSlices: Int): DC[T] = {
    val encoder = encFor[T]
    new ParallelCollectionDC(encoder, seq, Some(numSlices))
  }

  def textFile(path: String) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path)
    new SourceDC[String](encFor[String],path, sourceFunc, "textFile")
  }

  def textFile(path: String,
               minPartitions: Int) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path, minPartitions)
    new SourceDC[String](encFor[String], path, sourceFunc, "textFile")
  }

  def objectFile[T:Encoder](path: String) = {
    implicit val tClassTag = encFor[T].clsTag
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path)
    new SourceDC[T](encFor[T], path, sourceFunc, "objectFile")
  }

  def objectFile[T:Encoder](path: String,
                             minPartitions: Int) = {
    implicit val tClassTag = encFor[T].clsTag
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    new SourceDC[T](encFor[T], path, sourceFunc, "objectFile")
  }

  private[sparkflow] var checkpointDir = "/tmp/sparkflow"
  def setCheckpointDir(s: String) = {checkpointDir = s}

  private[sparkflow] var defaultPersistence = StorageLevel.MEMORY_AND_DISK
  def setPersistence(storageLevel: StorageLevel) = {defaultPersistence = storageLevel}

}
