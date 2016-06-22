package com.bloomberg

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.bloomberg.sparkflow.dc.{SourceDC, ParallelCollectionDC, DC}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.{RDDConversions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


import scala.language.implicitConversions

/**
  * Created by ngoehausen on 3/24/16.
  */
package object sparkflow {


//  private[sparkflow] var sqlContext: SQLContext = null

//  // This must live here to preserve binary compatibility with Spark < 1.5.
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  /**
    * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
    *
    * @since 1.3.0
    */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  def read = new DCDataFrameReader

  def parallelize[T:ClassTag](seq: Seq[T]): DC[T] = {
    new ParallelCollectionDC(seq)
  }

  def parallelize[T:ClassTag](seq: Seq[T], numSlices: Int): DC[T] = {
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

  def objectFile[T:ClassTag](path: String) = {
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path)
    new SourceDC[T](path, sourceFunc, "objectFile")
  }

  def objectFile[T:ClassTag](path: String,
                             minPartitions: Int) = {
    val sourceFunc = (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    new SourceDC[T](path, sourceFunc, "objectFile")
  }

  private[sparkflow] var checkpointDir = "/tmp/sparkflow"
  def setCheckpointDir(s: String) = {checkpointDir = s}

  private[sparkflow] var defaultPersistence = StorageLevel.MEMORY_AND_DISK
  def setPersistence(storageLevel: StorageLevel) = {defaultPersistence = storageLevel}

}
