package sparkflow.components

import sparkflow.layer.PD

/**
  * Created by ngoehausen on 3/2/16.
  */
trait Bundle {

  def calcElements() = {
    val c      = getClass
    val res = c.getMethods.map { m =>
      val name = m.getName
      val modifiers = m.getModifiers
      val types = m.getParameterTypes

      val isPD = classOf[PD[_]].isAssignableFrom(m.getReturnType)
      if (types.isEmpty && !java.lang.reflect.Modifier.isStatic(modifiers)
        && isPD) {
      m invoke this match {
        case pd: PD[_] => Some((name, pd))
        case _ => None
      }
      } else {
        None
      }
    }.filter(_.isDefined).map(_.get).toList

    res
  }

}
