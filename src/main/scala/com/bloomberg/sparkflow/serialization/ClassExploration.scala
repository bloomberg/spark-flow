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

import com.bloomberg.sparkflow.internal.Logging

/**
  * Created by ngoehausen on 4/11/16.
  */
object ClassExploration extends Logging {

  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  val APPLYS = Set("apply$mcB$sp", "apply$mcC$sp", "apply$mcD$sp", "apply$mcF$sp", "apply$mcI$sp", "apply$mcJ$sp",
                   "apply$mcS$sp", "apply$mcV$sp", "apply$mcZ$sp", "apply$mcDD$sp", "apply$mcDF$sp", "apply$mcDI$sp",
                   "apply$mcDJ$sp", "apply$mcFD$sp", "apply$mcFF$sp", "apply$mcFI$sp", "apply$mcFJ$sp", "apply$mcID$sp",
                   "apply$mcIF$sp", "apply$mcII$sp", "apply$mcIJ$sp", "apply$mcJD$sp", "apply$mcJF$sp", "apply$mcJI$sp",
                   "apply$mcJJ$sp", "apply$mcVD$sp", "apply$mcVF$sp", "apply$mcVI$sp", "apply$mcVJ$sp", "apply$mcZD$sp",
                   "apply$mcZF$sp", "apply$mcZI$sp", "apply$mcZJ$sp", "apply$mcDDD$sp", "apply$mcDDI$sp",
                   "apply$mcDDJ$sp", "apply$mcDID$sp", "apply$mcDII$sp", "apply$mcDIJ$sp", "apply$mcDJD$sp",
                   "apply$mcDJI$sp", "apply$mcDJJ$sp", "apply$mcFDD$sp", "apply$mcFDI$sp", "apply$mcFDJ$sp",
                   "apply$mcFID$sp", "apply$mcFII$sp", "apply$mcFIJ$sp", "apply$mcFJD$sp", "apply$mcFJI$sp",
                   "apply$mcFJJ$sp", "apply$mcIDD$sp", "apply$mcIDI$sp", "apply$mcIDJ$sp", "apply$mcIID$sp",
                   "apply$mcIII$sp", "apply$mcIIJ$sp", "apply$mcIJD$sp", "apply$mcIJI$sp", "apply$mcIJJ$sp",
                   "apply$mcJDD$sp", "apply$mcJDI$sp", "apply$mcJDJ$sp", "apply$mcJID$sp", "apply$mcJII$sp",
                   "apply$mcJIJ$sp", "apply$mcJJD$sp", "apply$mcJJI$sp", "apply$mcJJJ$sp", "apply$mcVDD$sp",
                   "apply$mcVDI$sp", "apply$mcVDJ$sp", "apply$mcVID$sp", "apply$mcVII$sp", "apply$mcVIJ$sp",
                   "apply$mcVJD$sp", "apply$mcVJI$sp", "apply$mcVJJ$sp", "apply$mcZDD$sp", "apply$mcZDI$sp",
                   "apply$mcZDJ$sp", "apply$mcZID$sp", "apply$mcZII$sp", "apply$mcZIJ$sp", "apply$mcZJD$sp",
                   "apply$mcZJI$sp", "apply$mcZJJ$sp")

  def getClassesAndSerializedFields(func: AnyRef): (List[Class[_]], List[String]) = {

    var toExplore = Set(func)
    var exploredClasses = Set[Class[_]]()
    var methodsEncountered = Set[OwnerName]()
    var encounteredSerializedFields = Set[String]()

    while (toExplore.nonEmpty) {
      val obj = toExplore.head
      toExplore = toExplore.tail

      logDebug(s"Exploring ${obj.getClass.getName}")

      exploredClasses = exploredClasses + obj.getClass

      val fieldObjects = getFieldObjects(obj)
      encounteredSerializedFields ++= fieldObjects.map(f => s"${obj.getClass.getName}||${f.toString}")
      val method = getMethodObjects(obj)
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
    println("cls: " + cls)

    val fields = cls.getDeclaredFields

//    fields.map(x => x.getName).foreach(println)

    val nestedObjects = fields
      .filter(_.getName != "serialVersionUID")
      .map(f => {
        f.setAccessible(true)
        f.get(func)
      }).filter(_ != null)

    nestedObjects.toSet
  }

  def getMethodObjects(func: AnyRef): Set[AnyRef] = {
    val cls = func.getClass

    val methods = cls.getDeclaredMethods

    println(methods.size)
    methods.map(x => x.toGenericString).foreach(println)
    methods.map(m => m.getDeclaringClass).foreach(println)
    methods.map(m => m.getName).foreach(println)

    methods.map(m => m.toString).foreach(println)


    val methodsUsed = getMethodsUsed(func)
    println(methodsUsed.size)
      methodsUsed.foreach(println)
    methods.toSet

//    val nestedMethods = methods
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

      toExplore = methods.flatMap(exploreMethod).filter(m => !methodsEncountered.contains(m))
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
    reader.accept(new ClassMethodExplorer(APPLYS, ownerNames), 0)
    ownerNames.toSet
  }

  def exploreMethod(ownerName: OwnerName) = {

    val moduleCls = Class.forName(ownerName.owner)
    val reader = getClassReader(moduleCls)
    val ownerNames = scala.collection.mutable.Set[OwnerName]()

    reader.accept(new ClassMethodExplorer(Set(ownerName.name), ownerNames), 0)
    ownerNames.toSet

  }

  def cleanClassName(className: String) = {
    className.replace('/', '.')
  }

  case class OwnerName(owner: String, name: String)

  class ClassMethodExplorer(methodNames: Set[String], ownerNames: scala.collection.mutable.Set[OwnerName]) extends ClassVisitor(ASM5) {

    override def visitMethod( access: Int,
                              name: String,
                              desc: String,
                              sig: String,
                              exceptions: Array[String]): MethodVisitor = {
      logDebug(s"Class visitMethod: name: $name, desc: $desc")
      if (name.startsWith("apply")) {
//      if (methodNames.contains(name)) {
        println("INSIDE VISITMETHOD: " + name)
        new MethodExplorer(ownerNames)
      } else {
        null
      }
    }

    override def visitField( access: Int,
                             name: String,
                             desc: String,
                             sig: String,
                             value: Object): FieldVisitor = {
      if (sig != null && value != null) {
        logDebug(s"visitField name: $name, desc: $desc, sig: $sig, value: ${value.getClass}: ${value.toString}")
      } else {
        logDebug(s"visitField name: $name, desc: $desc, sig: $sig")
      }
            new FieldExplorer(ownerNames)
//      return null
    }

  }

  class MethodExplorer(ownerNames: scala.collection.mutable.Set[OwnerName]) extends MethodVisitor(ASM5) {

    override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String, itf: Boolean) {
      ownerNames.add(OwnerName(cleanClassName(owner), name))
    }

//    override def visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
//      ownerNames.add(OwnerName(cleanClassName(owner), name))
//    }
  }

  class FieldExplorer(ownerNames: scala.collection.mutable.Set[OwnerName]) extends FieldVisitor(ASM5) {

    if (fv != null) {
      logDebug(s"FieldExplorer: ${fv.toString}")
    } else {
      logDebug("FieldExplorer is null")
    }

  }

}