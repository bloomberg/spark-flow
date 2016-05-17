package com.bloomberg.sparkflow.serialization

import org.objectweb.asm._
import org.objectweb.asm.Opcodes._
import reflect.runtime.{universe => ru}

import scala.util.Try

/**
  * Created by ngoehausen on 4/11/16.
  */
object ClassExploration {

  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  val APPLYMC2 = "apply$mcII$sp"

  def getClasses(func: AnyRef): Set[Class[_]] = {

    var toExplore = Set(func)
    var exploredClasses = Set[Class[_]]()
    var methodsEncountered = Set[OwnerName]()

    while(toExplore.nonEmpty){
      val obj = toExplore.head
      toExplore = toExplore.tail

      exploredClasses = exploredClasses + obj.getClass

      val fieldObjects = getFieldObjects(obj)
      val (methodObjects, methods) = getObjectsAndMethods(obj)
      methodsEncountered ++= methods

      val newFieldObjects = (fieldObjects ++ methodObjects).filter(fieldObject => {
        val clz = fieldObject.getClass
        !exploredClasses.contains(clz) && shouldExplore(clz)
      })

      toExplore ++= newFieldObjects
    }

    val classesFromMethods = methodsEncountered.map(_.owner).map(Class.forName)

    exploredClasses ++ classesFromMethods
  }

  def getClassReader(cls: Class[_]): ClassReader = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    new ClassReader(resourceStream)
  }

  def getFieldObjects(func: AnyRef): Set[AnyRef] = {
    val cls = func.getClass

    val fields = cls.getDeclaredFields

    val nestedObjects = fields.map(f =>{
      f.setAccessible(true)
      f.get(func)}).filter(_!= null)

    nestedObjects.toSet
  }

  def shouldExplore(clz: Class[_]) = {
    val name = clz.getName
    !(name.startsWith("java") || name.startsWith("scala"))
  }

  def hasFields(obj: AnyRef) = {
    Try{obj.getClass.getDeclaredFields}.isSuccess
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

    var toExplore = Try{getMethodsUsed(obj)}.toOption.toSet.flatten

    while(toExplore.nonEmpty) {

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
    reader.accept(new ClassMethodExplorer(APPLYMC2,ownerNames), 0)
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


  class ClassMethodExplorer(methodName: String, ownerNames: scala.collection.mutable.Set[OwnerName]) extends ClassVisitor {

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

    /** As seen from class ClassMethodExplorer, the missing signatures are as follows.
      *  For convenience, these are usable as stub implementations.
      */
    def visit(x$1: Int,x$2: Int,x$3: String,x$4: String,x$5: String,x$6: Array[String]): Unit = {}
    def visitAnnotation(x$1: String,x$2: Boolean): org.objectweb.asm.AnnotationVisitor = {null}
    def visitAttribute(x$1: org.objectweb.asm.Attribute): Unit = {}
    def visitEnd(): Unit = {}
    def visitField(x$1: Int,x$2: String,x$3: String,x$4: String,x$5: Any): org.objectweb.asm.FieldVisitor = {null}
    def visitInnerClass(x$1: String,x$2: String,x$3: String,x$4: Int): Unit = {}
    def visitOuterClass(x$1: String,x$2: String,x$3: String): Unit = {}
    def visitSource(x$1: String,x$2: String): Unit = {}

  }

  class MethodExplorer(ownerNames: scala.collection.mutable.Set[OwnerName]) extends MethodVisitor {

    override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
      ownerNames.add(OwnerName(cleanClassName(owner), name))
    }


    /** As seen from class MethodExplorer, the missing signatures are as follows.
      *  For convenience, these are usable as stub implementations.
      */
    def visitAnnotation(x$1: String,x$2: Boolean): org.objectweb.asm.AnnotationVisitor = {null}
    def visitAnnotationDefault(): org.objectweb.asm.AnnotationVisitor = {null}
    def visitAttribute(x$1: org.objectweb.asm.Attribute): Unit = {}
    def visitCode(): Unit = {}
    def visitEnd(): Unit = {}
    def visitFieldInsn(x$1: Int,x$2: String,x$3: String,x$4: String): Unit = {}
    def visitFrame(x$1: Int,x$2: Int,x$3: Array[Object],x$4: Int,x$5: Array[Object]): Unit = {}
    def visitIincInsn(x$1: Int,x$2: Int): Unit = {}
    def visitInsn(x$1: Int): Unit = {}
    def visitIntInsn(x$1: Int,x$2: Int): Unit = {}
    def visitJumpInsn(x$1: Int,x$2: org.objectweb.asm.Label): Unit = {}
    def visitLabel(x$1: org.objectweb.asm.Label): Unit = {}
    def visitLdcInsn(x$1: Any): Unit = {}
    def visitLineNumber(x$1: Int,x$2: org.objectweb.asm.Label): Unit = {}
    def visitLocalVariable(x$1: String,x$2: String,x$3: String,x$4: org.objectweb.asm.Label,x$5: org.objectweb.asm.Label,x$6: Int): Unit = {}
    def visitLookupSwitchInsn(x$1: org.objectweb.asm.Label,x$2: Array[Int],x$3: Array[org.objectweb.asm.Label]): Unit = {}
    def visitMaxs(x$1: Int,x$2: Int): Unit = {}
    def visitMultiANewArrayInsn(x$1: String,x$2: Int): Unit = {}
    def visitParameterAnnotation(x$1: Int,x$2: String,x$3: Boolean): org.objectweb.asm.AnnotationVisitor = {null}
    def visitTableSwitchInsn(x$1: Int,x$2: Int,x$3: org.objectweb.asm.Label,x$4: Array[org.objectweb.asm.Label]): Unit = {}
    def visitTryCatchBlock(x$1: org.objectweb.asm.Label,x$2: org.objectweb.asm.Label,x$3: org.objectweb.asm.Label,x$4: String): Unit = {}
    def visitTypeInsn(x$1: Int,x$2: String): Unit = {}
    def visitVarInsn(x$1: Int,x$2: Int): Unit = {}
  }

}