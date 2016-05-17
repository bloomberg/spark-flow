package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.CaseClasses._

import scala.util.Random


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

    val makeModel = cars.select("make", "model")
    val make = cars.select(cars("make")).checkpoint()

    assert(make.getSignature != makeModel.getSignature)

    make.getDF(sc).foreach(println)
  }

  test("json"){
    val path = "test.json"

    val dc = read.json(testFile(path)).repartition(10)

    val providerURLS = dc.select("provider_urls").checkpoint()

    providerURLS.getDF(sc).show()
    println(dc.getRDD(sc).first())
  }

  test("fromRdd"){
    val trashFires = parallelize(1 to 10)
      .map(_ => TrashFire(Random.nextDouble(), Random.nextDouble()))

    val dfdc = trashFires.toDF()
    dfdc.select("temp").getDF(sc).show()

  }

}