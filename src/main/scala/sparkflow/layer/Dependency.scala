package sparkflow.layer

import scala.reflect._
/**
  * Created by ngoehausen on 3/23/16.
  */
abstract class Dependency[T: ClassTag] extends Serializable{

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
