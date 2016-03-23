package sparkflow

import org.apache.spark.SparkContext
import sparkflow.layer.{SourceDC, DC, ParallelCollectionDC}

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
object FlowFuncs {
  val sentinelInt = -1

  def parallelize[T:ClassTag](seq: Seq[T]): DC[T] = {
    new ParallelCollectionDC(seq)
  }

  def textFile(path: String,
               minPartitions: Int = sentinelInt) = {
    val sourceFunc = if(minPartitions == sentinelInt){
      (sc: SparkContext) => sc.textFile(path)
    } else {
      (sc: SparkContext) => sc.textFile(path, minPartitions)
    }
    new SourceDC[String](path, sourceFunc, "textFile")
  }

  def objectFile[T:ClassTag](path: String,
               minPartitions: Int = sentinelInt) = {
    val sourceFunc = if(minPartitions == sentinelInt){
      (sc: SparkContext) => sc.objectFile[T](path)
    } else {
      (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    }
    new SourceDC[T](path, sourceFunc, "textFile")
  }

}
