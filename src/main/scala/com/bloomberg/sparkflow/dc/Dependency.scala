package com.bloomberg.sparkflow.dc

/**
  * Created by ngoehausen on 3/23/16.
  */
abstract class Dependency[T](val parents: Seq[Dependency[_]]) extends Serializable {

  val children = scala.collection.mutable.Set[Dependency[_]]()
  parents.map(_.children.add(this))

  //  val ct = classTag[T]
  protected def computeSignature(): String

  private var signature: String = _


  def getSignature: String = {
    if (signature == null) {
      this.signature = this.computeSignature()
    }
    signature
  }

}
