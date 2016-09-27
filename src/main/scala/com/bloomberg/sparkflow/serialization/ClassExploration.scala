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

package com.bloomberg.sparkflow.serialization

import org.objectweb.asm.Opcodes._
import org.objectweb.asm._

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

/**
  * Created by ngoehausen on 4/11/16.
  */
object ClassExploration {

  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  val APPLYMC2 = "apply$mcII$sp"

  def getClassesAndSerializedFields(func: AnyRef): (List[Class[_]], List[String]) = {

    var toExplore = Set(func)
    var exploredClasses = Set[Class[_]]()
    var methodsEncountered = Set[OwnerName]()
    var encounteredSerializedFields = Set[String]()

    while (toExplore.nonEmpty) {
      val obj = toExplore.head
      toExplore = toExplore.tail

      exploredClasses = exploredClasses + obj.getClass

      val fieldObjects = getFieldObjects(obj)
      encounteredSerializedFields ++= fieldObjects.map(f => s"${obj.getClass.getName}||${f.toString}")
      val (methodObjects, methods) = getObjectsAndMethods(obj)
      methodsEncountered ++= methods

      val newFieldObjects = (fieldObjects ++ methodObjects).filter(fieldObject => {
        val clz = fieldObject.getClass
        !exploredClasses.contains(clz) && shouldExplore(clz)
      })

      toExplore ++= newFieldObjects
    }

    val classesFromMethods = methodsEncountered.map(_.owner).map(Class.forName)

    val resultClasses = (exploredClasses ++ classesFromMethods).toList.sortBy(_.getName)
    val resultFields = encounteredSerializedFields.toList.sorted
    (resultClasses, resultFields)
  }

  def getClassReader(cls: Class[_]): ClassReader = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    new ClassReader(resourceStream)
  }

  def getFieldObjects(func: AnyRef): Set[AnyRef] = {
    val cls = func.getClass

    val fields = cls.getDeclaredFields

    val nestedObjects = fields
      .filter(_.getName != "serialVersionUID")
      .map(f => {
        f.setAccessible(true)
        f.get(func)
      }).filter(_ != null)

    nestedObjects.toSet
  }

  def shouldExplore(clz: Class[_]) = {
    val name = clz.getName
    !(name.startsWith("java") || name.startsWith("scala"))
  }

  def hasFields(obj: AnyRef) = {
    Try {
      obj.getClass.getDeclaredFields
    }.isSuccess
  }

  def getObjFromModule(moduleName: String, name: String): AnyRef = {
    val mod = getModuleByName(moduleName)
    val f = mod.getClass.getDeclaredField(name)
    f.setAccessible(true)
    f.get(mod)
  }

  def getObjectsAndMethods(obj: AnyRef): (Set[AnyRef], Set[OwnerName]) = {

    var objectsEncountered = Set(obj)
    var methodsEncountered = Set[OwnerName]()

    var toExplore = Try {
      getMethodsUsed(obj)
    }.toOption.toSet.flatten

    while (toExplore.nonEmpty) {

      val objects = toExplore.flatMap(ownerName => Try {
        getObjFromModule(ownerName.owner, ownerName.name)
      }.toOption)

      objectsEncountered ++= objects

      val methods = toExplore.filter(ownerName => Try {
        getObjFromModule(ownerName.owner, ownerName.name)
      }.isFailure)

      methodsEncountered ++= methods

      toExplore = methods.flatMap(exploreMethod)

    }

    (objectsEncountered, methodsEncountered)

  }

  def getModuleByName(moduleName: String) = {
    val res = mirror.staticModule(moduleName)
    mirror.reflectModule(res).instance.asInstanceOf[AnyRef]
  }

  def hashValue(obj: AnyRef): String = {
    s"${obj.getClass.getTypeName}:${obj.toString}"
  }

  def getMethodsUsed(obj: AnyRef): Set[OwnerName] = {
    val reader = getClassReader(obj.getClass)
    val ownerNames = scala.collection.mutable.Set[OwnerName]()
    reader.accept(new ClassMethodExplorer(APPLYMC2, ownerNames), 0)
    ownerNames.toSet
  }

  def exploreMethod(ownerName: OwnerName) = {

    val moduleCls = Class.forName(ownerName.owner)
    val reader = getClassReader(moduleCls)
    val ownerNames = scala.collection.mutable.Set[OwnerName]()

    reader.accept(new ClassMethodExplorer(ownerName.name, ownerNames), 0)
    ownerNames.toSet

  }

  def cleanClassName(className: String) = {
    className.replace('/', '.')
  }

  case class OwnerName(owner: String, name: String)


  class ClassMethodExplorer(methodName: String, ownerNames: scala.collection.mutable.Set[OwnerName]) extends ClassVisitor(ASM5) {

    override def visitMethod(
                              access: Int,
                              name: String,
                              desc: String,
                              sig: String,
                              exceptions: Array[String]): MethodVisitor = {
      //      println(s"Class visitMethod: name: $name, desc: $desc")
      if (name == methodName) {
        new MethodExplorer(ownerNames)
      } else {
        null
      }
    }

  }

  class MethodExplorer(ownerNames: scala.collection.mutable.Set[OwnerName]) extends MethodVisitor(ASM5) {

    override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String, itf: Boolean) {
      ownerNames.add(OwnerName(cleanClassName(owner), name))
    }

  }

}