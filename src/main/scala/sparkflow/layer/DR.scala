package sparkflow.layer

import org.apache.spark.SparkContext
import scala.reflect.ClassTag

/**
  * Deferred Result
  */
abstract class DR[U:ClassTag](dep: DC[_]) extends Dependency[U](Seq(dep)) {

  def get(sc: SparkContext): U

}
