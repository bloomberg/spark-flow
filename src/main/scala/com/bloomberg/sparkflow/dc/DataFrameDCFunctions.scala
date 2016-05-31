package com.bloomberg.sparkflow.dc

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Created by ngoehausen on 4/27/16.
  */
class DataFrameDCFunctions(self: DC[Row]) {

  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val schema = self.getSchema(sc)
    assert(schema.isDefined)
    sqlContext.createDataFrame(self.getRDD(sc), self.getSchema(sc).get)
  }

  @scala.annotation.varargs
  def select(cols: Column*): DC[Row] = {
    val f = (df: DataFrame) => {
      df.select(cols:_*)
    }
    val hashTarget = cols.map(_.toString())
    new DataFrameTransformDC(self, f, hashTarget)
  }

  @scala.annotation.varargs
  def select(col: String, cols: String*): DC[Row] = {
    val f = (df: DataFrame) => {
      df.select(col, cols:_*)
    }
    val hashTarget = Seq("select", col) ++ cols

    new DataFrameTransformDC(self, f, hashTarget)
  }

  def selectExpr(exprs: String*): DC[Row] = {
    val f = (df: DataFrame) => {
      df.selectExpr(exprs:_*)
    }
    val hashTarget = Seq("selectExpr") ++ exprs
    new DataFrameTransformDC(self, f, hashTarget)
  }

  def filter(condition: Column): DC[Row] =  {
    val f = (df: DataFrame) => {
      df.filter(condition)
    }

    val hashTarget = Seq("filter", condition.toString())
    new DataFrameTransformDC(self, f, hashTarget)
  }

  def unionAll(other: DC[Row]): DC[Row] = {
    new UnionDC[Row](self, other)
  }

  def drop(colName: String): DC[Row] = {
    val f = (df: DataFrame) => {
      df.drop(colName)
    }
    val hashTarget = Seq("drop", colName)
    new DataFrameTransformDC(self, f, hashTarget)
  }

  def apply(colName: String) = {
    new Column(colName)
  }

  def join(right: DC[Row]): DC[Row] = {
    val f = (left: DataFrame, right: DataFrame) => {
      left.join(right)
    }
    val hashTarget = Seq("join")
    new MultiDFTransformDC(self, right, f, hashTarget)
  }

  def join(right: DC[Row], usingColumn: String): DC[Row] = join(right, usingColumn)

  def join(right: DC[Row], joinExprs: Column): DC[Row] = join(right, joinExprs, "inner")

  def join(right: DC[Row], joinExprs: Column, joinType: String): DC[Row] = {
    val f = (left: DataFrame, right: DataFrame) => {
      left.join(right, joinExprs, joinType)
    }
    val hashTarget = Seq("join", joinType, joinExprs.toString())
    new MultiDFTransformDC(self, right, f, hashTarget)
  }

}
