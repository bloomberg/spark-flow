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

package com.bloomberg.sparkflow.graphs

import com.bloomberg.sparkflow.dc.Dependency

import scala.collection.mutable._

/**
  * Created by amohindra2 on 8/4/16.
  */


object Util {

  case class Edge(src: String, dest: String)

  case class Node(id: String)

  case class Graph(nodes: Set[Node], edges: Set[Edge])

  def buildDAG(start: Dependency[_]): Graph = {

    var explored: Set[Dependency[_]] = Set()
    var toExplore: Set[Dependency[_]] = Set(start)

    while (toExplore.nonEmpty) {
      val x = toExplore.head
      val goodneighbors = (x.parents ++ x.children).toSet -- explored
      toExplore = toExplore ++ goodneighbors
      explored = explored + x
      toExplore = toExplore - x
    }

    val nodes = explored.map(x => Node(x.getSignature))
    val edges = explored.flatMap(x => x.parents.map(y => Edge(y.getSignature, x.getSignature)))

    Graph(nodes, edges)
  }

}
