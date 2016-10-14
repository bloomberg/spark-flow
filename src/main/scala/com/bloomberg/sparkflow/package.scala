/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bloomberg

import com.bloomberg.sparkflow.dc.{DC, ParallelCollectionDC, SourceDC}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.EncoderUtil.encoderFor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions

/**
  * Created by ngoehausen on 3/24/16.
  */
package object sparkflow extends SQLImplicits {

  private[sparkflow] def setSession(spark: SparkSession): Unit = {
    _spark = spark
    sqlContext = SQLContext.getOrCreate(spark.sparkContext)
  }

  private[sparkflow] def setSession(sc: SparkContext): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    setSession(spark)
  }

  private[sparkflow] def getSpark(sc: SparkContext): SparkSession = {
    synchronized {
      setSession(sc)
      spark
    }
  }

  private var _spark: SparkSession = null
  private var sqlContext: SQLContext = null

  protected override def _sqlContext: SQLContext = sqlContext

  private def spark: SparkSession = _spark

  implicit def denseVectorEncoder = org.apache.spark.sql.Encoders.kryo[DenseVector]

  implicit def sparseVectorEncoder = org.apache.spark.sql.Encoders.kryo[SparseVector]

  implicit def vectorEncoder = org.apache.spark.sql.Encoders.kryo[Vector]

  private[sparkflow] val emptyRowEncoder = RowEncoder(new StructType())

  def read = new DCDataFrameReader

  def parallelize[T: Encoder](seq: Seq[T]): DC[T] = {
    val encoder = encoderFor[T]
    new ParallelCollectionDC(encoder, seq, None)
  }

  def parallelize[T: Encoder](seq: Seq[T], numSlices: Int): DC[T] = {
    val encoder = encoderFor[T]
    new ParallelCollectionDC(encoder, seq, Some(numSlices))
  }

  def textFile(path: String) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path)
    new SourceDC[String](encoderFor[String], path, sourceFunc, "textFile")
  }

  def textFile(path: String,
               minPartitions: Int) = {
    val sourceFunc = (sc: SparkContext) => sc.textFile(path, minPartitions)
    new SourceDC[String](encoderFor[String], path, sourceFunc, "textFile")
  }

  def objectFile[T: Encoder](path: String) = {
    implicit val tClassTag = encoderFor[T].clsTag
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path)
    new SourceDC[T](encoderFor[T], path, sourceFunc, "objectFile")
  }

  def objectFile[T: Encoder](path: String,
                             minPartitions: Int) = {
    implicit val tClassTag = encoderFor[T].clsTag
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    new SourceDC[T](encoderFor[T], path, sourceFunc, "objectFile")
  }

  private[sparkflow] var checkpointDir = "/tmp/sparkflow"

  def setCheckpointDir(s: String) = {
    checkpointDir = s
  }

  private[sparkflow] var defaultPersistence = StorageLevel.MEMORY_AND_DISK

  def setPersistence(storageLevel: StorageLevel) = {
    defaultPersistence = storageLevel
  }

  private[sparkflow] var autoCachingEnabled = true

  def setAutoCaching(enabled: Boolean) = {
    autoCachingEnabled = enabled
  }

}
