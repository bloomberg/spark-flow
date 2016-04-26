package com.bloomberg.sparkflow.dc

import scala.reflect._
/**
  * Created by ngoehausen on 3/23/16.
  */
abstract class Dependency[T: ClassTag](val parents: Seq[Dependency[_]]) extends Serializable{

  val children = scala.collection.mutable.Set[Dependency[_]]()
  parents.map(_.children.add(this))

  val ct = classTag[T]
  protected def computeHash(): String
  private var hash: String = _


  def getHash: String = {
    if(hash == null){
      this.hash = this.computeHash()
    }
    hash
  }

}
