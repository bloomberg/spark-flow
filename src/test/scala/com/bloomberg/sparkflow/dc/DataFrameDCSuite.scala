package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 4/26/16.
  */
class DataFrameDCSuite extends FunSuite with SharedSparkContext with ShouldMatchers {

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  test("csv"){
    val cars = read
      .format("csv")
      .option("header", "true")
      .load(testFile("cars.csv"))
    val df = cars.getDF(sc)
    df.show()

    df.columns

    val makeModel = cars.select("make", "model")

    val make = cars.select("make")
    val tesla = cars.filter(cars("make") === "Tesla")
    tesla.getDF(sc).show()

    println(makeModel.getSignature)
    println(make.getSignature)
  }


  test("json"){
    val path = "test.json"

    val dc = read.json(testFile(path))
    println(dc.getRDD(sc).first())
  }
}
