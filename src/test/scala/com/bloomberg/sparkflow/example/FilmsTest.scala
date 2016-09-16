package com.bloomberg.sparkflow.example

import com.bloomberg.sparkflow.example.FilmsPipeline.FilmMain
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

/**
  * Created by ngoehausen on 8/26/16.
  */
class FilmsTest extends FunSuite with SharedSparkContext  {
  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  test("pipeline"){

    val filmPipe = new FilmMain
    filmPipe.filmRows.getDF(sc).show()

    filmPipe.topActors.get(sc).foreach(println)
  }
}
