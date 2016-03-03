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
      case TransformType.Filter => handleFilter(compactPD, sc)
    }
  }

  private def handleMap(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    val rddDepends = compactPD.parents.map(getRDD(_,sc))
    assert(rddDepends.size == 1)

    val f = stringToObj[Any => Any](compactPD.transform.encodedTransform)
    val prev = rddDepends.head
    prev.map(f)
  }

  private def handleFilter(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    val rddDepends = compactPD.parents.map(getRDD(_,sc))
    assert(rddDepends.size == 1)

    val f = stringToObj[Any => Boolean](compactPD.transform.encodedTransform)
    val prev = rddDepends.head
    prev.filter(f)
  }

  private def handleParallelize(compactPD: CompactPD, sc: SparkContext): RDD[_] = {
    val seq = stringToObj[Seq[Any]](compactPD.transform.encodedTransform)
    sc.parallelize(seq)
  }

}
