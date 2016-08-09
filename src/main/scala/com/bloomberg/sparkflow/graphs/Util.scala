package com.bloomberg.sparkflow.graphs

import com.bloomberg.sparkflow._
import com.bloomberg.sparkflow.dc.{DC, Dependency}
import scala.collection.mutable.ListBuffer

/**
  * Created by amohindra2 on 8/4/16.
  */


object Util {

  //Find top-level nodes.
  def getGrandparents(x: Dependency[_]) : List[Dependency[_]] ={
    var temp = new ListBuffer[Dependency[_]]

    def grandFinder(y: Dependency[_]){
      if(y.parents.nonEmpty){
        for(a <- y.parents){
          grandFinder(a)
        }
      } else {
        temp += y
      }
    }
    grandFinder(x)

    temp.toList.distinct
  }


  //Build DAG from given starter node. Depends on getGrandparents.
  def buildDAG(starter: Dependency[_]) : Graph ={
    val grands = getGrandparents(starter)

    val nodeMap = scala.collection.mutable.HashMap.empty[String, Node]
    var edgeList = new ListBuffer[Edge]

    def buildDAG2(x: Dependency[_]){
      nodeMap.update(x.getSignature , new Node(x.getSignature))

      if(x.children.nonEmpty){
        for(a <- x.children){
          edgeList += new Edge(x.getSignature, a.getSignature)
          buildDAG2(a)
        }
      }

    }

    for (b <- grands){
      buildDAG2(b)
    }

    new Graph(nodeMap, edgeList.toList)

  }

}
