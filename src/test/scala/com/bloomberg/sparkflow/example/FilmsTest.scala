/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    filmPipe.filmsWithTopActors.getDataset(sc).show()
    println(filmPipe.filmsWithTopActors.count.get(sc))
  }
}
