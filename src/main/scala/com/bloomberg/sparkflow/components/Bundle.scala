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

package com.bloomberg.sparkflow.components

import com.bloomberg.sparkflow.dc.DC

/**
  * Created by ngoehausen on 3/2/16.
  */
trait Bundle {

  private val keywords = Set("calcElements", "toString", "hashCode", "getClass", "elements")

  lazy val elements = calcElements()

  protected def calcElements() = {
    val c = getClass
    val res = c.getMethods.map { m =>
      val name = m.getName
      val modifiers = m.getModifiers
      val types = m.getParameterTypes

      val isPD = classOf[DC[_]].isAssignableFrom(m.getReturnType)
      if (types.isEmpty && !java.lang.reflect.Modifier.isStatic(modifiers)
        && isPD) {
        m invoke this match {
          case pd: DC[_] => Some((name, pd))
          case _ => None
        }
      } else {
        None
      }
    }.filter(_.isDefined).map(_.get).toList

    res
  }


  def getStuff(): List[Map[String, Any]] = {
    val c = getClass
    val res = c.getMethods.map { m =>

      val map = Map(
        "name" -> m.getName,
        "modfiers" -> m.getModifiers,
        "parameterTypes" -> m.getParameterTypes.toList.mkString(" "),
        "returnType" -> m.getReturnType,
        "parameterCount" -> m.getParameterCount,
        "getDeclaringClass" -> m.getDeclaringClass,
        "isDefault" -> m.isDefault,
        "isSynthetic" -> m.isSynthetic,
        "isVarArgs" -> m.isVarArgs,
        "isAccessible" -> m.isAccessible,
        "getDefaultValue" -> m.getDefaultValue,
        "getGenericReturnType" -> m.getGenericReturnType,
        "toGenericString" -> m.toGenericString,
        "isBridge" -> m.isBridge,
        "getDeclaredAnnotations" -> m.getDeclaredAnnotations.mkString(" "),
        "getAnnotations" -> m.getAnnotations.mkString(" ")
      )

      map
    }.toList

    res
  }

  def printStuff() = {
    val stuff = getStuff().filter(filterMaps)

    val filterFunc = (m: Map[String, Any]) => m.get("name") == Some("anInt") || m.get("name") == Some("nums")
    val stuffIWant = stuff.filter {
      filterFunc
    }
    val stuffIDontWant = stuff.filter { m => !filterFunc(m) }

    stuffIWant.foreach { map =>
      println("-" * 40)
      map.foreach { case (name, thing) => println(s"$name -> $thing") }
    }

    println("\n\n")

    stuffIDontWant.foreach { map =>
      println("-" * 40)
      map.foreach { case (name, thing) => println(s"$name -> $thing") }
    }


  }


  def filterMaps = (m: Map[String, Any]) => {
    m.get("parameterCount") == Some(0) &&
      m.get("returnType") != Some(Void.TYPE) &&
      !keywords.contains(m.get("name").get.toString)
  }

}
