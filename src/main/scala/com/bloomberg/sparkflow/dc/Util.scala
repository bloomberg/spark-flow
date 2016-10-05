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

package com.bloomberg.sparkflow.dc


import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.reflect.{ClassTag, classTag}

/**
  * Created by ngoehausen on 5/18/16.
  */
object Util {


  private[dc] def saveCheckpoint[T: ClassTag](checkpointPath: String, dataset: Dataset[T]) = {
    assert(dataset != null)
    dataset.write.mode(SaveMode.Overwrite).parquet(checkpointPath)
  }

  private[dc] def loadCheckpoint[T: ClassTag](checkpointPath: String, spark: SparkSession)(implicit tEncoder: Encoder[T]): Option[Dataset[T]] = {
    if (pathExists(checkpointPath, spark.sparkContext)) {
      val dataFrame = spark.read.parquet(checkpointPath)
      val dataset = if (tEncoder.clsTag.equals(classTag[Row])) {
        dataFrame.asInstanceOf[Dataset[T]]
      } else {
        dataFrame.as[T]
      }
      dataset.count()
      Some(dataset)
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
