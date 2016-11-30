package com.bloomberg.sparkflow.dc

import org.apache.spark.sql.{Column, Dataset, Row}

/**
  * Created by rely10 on 11/30/16.
  */
class DataFrameFunctions(self: DC[Row]) {

    def join(right: DC[Row]): DC[Row] = {
      val f = (left: Dataset[_], right: Dataset[_]) => {
        left.join(right)
      }
      val hashTarget = Seq("join")
      new MultiDatasetTransformDC(self, right, f, hashTarget)
    }

    def join(right: DC[Row], usingColumn: String): DC[Row] = {
      val f = (left: Dataset[_], right: Dataset[_]) => {
        left.join(right, usingColumn)
      }
      val hashTarget = Seq("join", usingColumn)
      new MultiDatasetTransformDC(self, right, f, hashTarget)
    }

    def join(right: DC[Row], joinExprs: Column): DC[Row] = join(right, joinExprs, "inner")

    def join(right: DC[Row], joinExprs: Column, joinType: String): DC[Row] = {
      val f = (left: Dataset[_], right: Dataset[_]) => {
        left.join(right, joinExprs)
      }
      val hashTarget = Seq("join", joinType, joinExprs.toString())
      new MultiDatasetTransformDC(self, right, f, hashTarget)
    }


}
