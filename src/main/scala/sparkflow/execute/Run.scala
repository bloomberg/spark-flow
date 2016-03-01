package sparkflow.execute

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sparkflow.serialization.Formats.{TransformType, CompactPD}
import org.apache.spark.hax.SerializeUtil._

/**
  * Created by ngoehausen on 3/1/16.
  */
object Run {

  def getRDD(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    compactPD.transform.transformType match {
      case TransformType.Map => handleMap(compactPD, sc)
      case TransformType.Parallelize => handleParallelize(compactPD, sc)
    }
  }

  def handleMap(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    val rddDepends = compactPD.parents.map(getRDD(_,sc))
    assert(rddDepends.size == 1)

    val f = stringToObj[Any => Any](compactPD.transform.encodedTransform)
    val prev = rddDepends.head
    prev.map(f)
  }

  def handleParallelize(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    val seq = stringToObj[Seq[Any]](compactPD.transform.encodedTransform)
    sc.parallelize(seq)
  }

}
