package com.bloomberg.sparkflow.dc

import org.apache.spark.sql.Row
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by ngoehausen on 5/11/16.
  */
class ProductDCFunctions[T <: Product : TypeTag](self: DC[T]) {

  def toDF(): DC[Row] = {
    new RDDToDFTransformDC[T](self)
  }

}
