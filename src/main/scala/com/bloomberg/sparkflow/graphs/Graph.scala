package com.bloomberg.sparkflow.graphs

import scala.collection.mutable

/**
  * Created by amohindra2 on 8/4/16.
  */
class Graph(nodeMap: mutable.HashMap[String, Node], edgeList: List[Edge]){

  val nodemap = nodeMap
  val nodes = nodeMap.values.toList
  val edges = edgeList


}
