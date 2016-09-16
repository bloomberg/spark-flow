package com.bloomberg.sparkflow

/**
  * Created by ngoehausen on 5/11/16.
  */
object CaseClasses extends Serializable {

  case class TrashFire(temp: Double, mass: Double)
  case class Stuff(provider_urls: Array[String])

}
