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

import com.bloomberg.sparkflow
import com.bloomberg.sparkflow._

    /**
      * Created by ngoehausen on 9/14/16.
      */
    object FilmsPipeline {

      class FilmMain(){

        val inputFilmRows = sparkflow.read.format("csv").option("header", "true")
          .load(testFile("Film_Locations_in_San_Francisco.csv"))
          .toDF(FilmsPipeline.columns:_*)
          .as[FilmsPipeline.InputFilmRow]

        val filmRows = inputFilmRows.keyBy(_.title)
          .groupByKey()
          .map(parseFilmRows)
          .checkpoint()

        val actorMovieCount = filmRows.flatMap(filmRow => filmRow.actors.map((_,1))).reduceByKey(_+_)
        val topActors = actorMovieCount.sortBy(_._2, ascending = false).map(_._1).take(5)

        val filmsWithTopActors = filmRows.withResult(topActors).filter{
          filmRowActors => {
            val (filmRow, actors) = filmRowActors
            filmRow.actors.toSet.intersect(actors.toSet).nonEmpty
          }
        }.map(_._1)
      }


      val columns = Seq(
        "title",
        "release",
        "locations",
        "funFacts",
        "productionCompany",
        "distributor",
        "director",
        "writer",
        "actor1",
        "actor2",
        "actor3"
      )

      case class InputFilmRow(title: String,
                              release: String,
                              locations: String,
                              funFacts: String,
                              productionCompany: String,
                              distributor: String,
                              director: String,
                              writer: String,
                              actor1: String,
                              actor2: String,
                              actor3: String)

      case class FilmRow( title: String,
                          release: String,
                          locations: Seq[String],
                          funFacts: String,
                          productionCompany: String,
                          distributor: String,
                          director: String,
                          writer: String,
                          actors: Seq[String])


      val parseFilmRows = (tuple: (String, Seq[InputFilmRow])) => {
        val (title, rows) = tuple
        val firstRow = rows.head
        val locations = rows.map(_.locations).distinct
        val actors = rows.flatMap(row => Seq(row.actor1, row.actor2, row.actor3)).distinct.filter(_ != "")
        FilmRow(
          firstRow.title,
          firstRow.release,
          locations,
          firstRow.funFacts,
          firstRow.productionCompany,
          firstRow.distributor,
          firstRow.director,
          firstRow.writer,
          actors)
      }

      def testFile(fileName: String): String = {
        Thread.currentThread().getContextClassLoader.getResource(fileName).toString
      }


    }
