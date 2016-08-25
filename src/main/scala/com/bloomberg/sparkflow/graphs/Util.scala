package com.bloomberg.sparkflow.graphs

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.dc.{DC, Dependency}
import scala.collection.mutable._

/**
  * Created by amohindra2 on 8/4/16.
  */


object Util {

  case class Edge(src: String, dest: String)
  case class Node(id: String)
  case class Graph(nodes: Set[Node], edges: Set[Edge])

  def buildDAG(start: Dependency[_]) : Graph = {

    var explored : Set[Dependency[_]] = Set()
    var toExplore : Set[Dependency[_]] = Set(start)

    while(toExplore.nonEmpty){
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
