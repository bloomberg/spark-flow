package sparkflow

import sparkflow.layer.ParallelCollectionPD

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
object FlowFuncs {

  def parallelize[T:ClassTag](seq: Seq[T]) = {
    new ParallelCollectionPD(seq)
  }
}
