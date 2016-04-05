package sparkflow.serialization

/**
  * Created by ngoehausen on 3/30/16.
  */

import scala.runtime.AbstractFunction1

import org.apache.spark.hax.MyClosureCleaner
import org.objectweb.asm.Opcodes.ASM5
import org.objectweb.asm._

import scala.collection.mutable.{Set, Map}

object ByteCodeStuff {

  def instantiateClass(
                                cls: Class[_],
                                enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }


  def exploreClass(cls: Class[_]): Unit = {
    val declaredFields = cls.getDeclaredFields
    val declaredClasses = cls.getDeclaredClasses

    println(s"declaredFields: ${declaredFields.size}")
    println(s"declaredClasses: ${declaredClasses.size}")

    declaredFields.foreach(f => println(s"declared field: $f"))
    declaredClasses.foreach(f => println(s"declared class: $f"))

    val fields = declaredFields.toList
//    val field2 = fields(1)
//    println(field2.getName)
//    println(field2.getDeclaringClass.getName)
//
//    println(field2)


//    val clz = Class.forName(field2.getDeclaringClass.getName + "." + field2.getName,
//      false,
//      Thread.currentThread.getContextClassLoader)
//    println(clz)
//    val classes = declaredFields.map(f => f.getType)
//    classes.foreach(clz => exploreClass(clz))
    val reader = getClassReader(cls)
    reader.accept(new DependencyVisitor(),0)
  }

  def exploreObj(obj: AnyRef) = {


    val cls = obj.getClass
    exploreClass(cls)


  }

  def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    new ClassReader(resourceStream)
  }


  def aMethod(x: Int) = x / 2

  def main(args: Array[String]) {
    val evenMoreNested = (x: Int) => x + 7
    val nested = (x: Int) => evenMoreNested(x) + 3
    val g = (x: Int) =>  3 + nested(x)


//    MyClosureCleaner.clean(g)
    exploreObj(g)
  }

//
//  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
//    val seen = Set[Class[_]](obj.getClass)
//    var stack = List[Class[_]](obj.getClass)
//    while (!stack.isEmpty) {
//      val cr = getClassReader(stack.head)
//      stack = stack.tail
//      val set = Set[Class[_]]()
//      cr.accept(new InnerClosureFinder(set), 0)
//      for (cls <- set -- seen) {
//        seen += cls
//        stack = cls :: stack
//      }
//    }
//    (seen - obj.getClass).toList
//  }

}


class FieldAccessFinder()
  extends ClassVisitor(ASM5) {

  override def visitMethod(
                            access: Int,
                            name: String,
                            desc: String,
                            sig: String,
                            exceptions: Array[String]): MethodVisitor = {
    println(s"visitMethod: name: $name, desc: $desc")


    // If we are told to visit only a certain method and this is not the one, ignore it

    new MethodVisitor(ASM5) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        println(s"visiting field: op: $op, owner: $owner, name; $name, desc: $desc")

      }

      override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
        println(s"visiting method insn: opcode: $opcode, owner: $owner, name; $name, desc: $desc")
      }
    }
  }
}



class DependencyVisitor extends ClassVisitor(ASM5){

  private var myName: String = _

  override def visit(version: Int, access: Int, name: String, sig: String,
                     superName: String, interfaces: Array[String]) {
    println(s"Class visit: name: $name, superName: $superName")
    myName = name

  }

  override def visitSource (source: String, debug: String) {
    println(s"Class visitSource: source: $source, debug: $debug")
  }

  override def visitOuterClass (owner: String, name: String, desc: String) {
    println(s"Class visitOuterClass: owner: $owner, name: $name, desc: $desc")
  }

  override def visitMethod(
                            access: Int,
                            name: String,
                            desc: String,
                            sig: String,
                            exceptions: Array[String]): MethodVisitor = {
    println(s"Class visitMethod: name: $name, desc: $desc")
    new MethodDependencyVisitor(myName)
  }

  override def visitField(
                            access: Int,
                            name: String,
                            desc: String,
                            sig: String,
                            value: Object): FieldVisitor = {
    println(s"Class visitField: name: $name, desc: $desc, value: $value")
    new FieldDependencyVisitor
  }

  override def visitInnerClass (name: String, outerName: String, innerName: String, access: Int) {
    println(s"Class visitInnerClass: name: $name, outerName: $outerName, innerName: $innerName")
  }


  override def visitAttribute (attr: Attribute) {
    println(s"Class visitAttribute: attr: $attr")
  }

  override def visitAnnotation(
                            desc: String,
                            visible: Boolean): AnnotationVisitor = {
    println(s"Class visitAnnotation: desc: $desc, visible: $visible")
    new AnnotationDependencyVisitor
  }

}
class MethodDependencyVisitor(owner: String) extends MethodVisitor(ASM5){

  override def visitParameterAnnotation(
                               parameter: Int,
                                desc: String,
                                visible: Boolean): AnnotationVisitor = {
    println(s"Method visitAnnotation: parameter: $parameter, desc: $desc, visible: $visible")
    new AnnotationDependencyVisitor
  }

  override def visitLocalVariable (name: String, desc: String, signature: String, start: Label, end: Label, index: Int) {
    println(s"Method visitLocalVariable: name: $name, desc: $desc, signature: $signature, start: $start, end: $end, index: $index,")
//    println(Class.forName(owner + "$$" + name))
  }

  override def visitAttribute (attr: Attribute) {
    println(s"Method visitAttribute: attr: $attr")
  }

  override def visitCode() {
    println("Method visitCode")
  }

  override def visitFrame (`type`: Int, nLocal: Int, local: Array[AnyRef], nStack: Int, stack: Array[AnyRef]) {
    println(s"Method visitFrame: `type`: ${`type`}, nLocal: $nLocal, local: $local,  nStack: $nStack, stack: $stack")
  }

  override def visitVarInsn (opcode: Int, `var`: Int) {
    println(s"Method visitVarInsn: opcode: $opcode, `var`: ${`var`}")
  }

  override def visitTypeInsn (opcode: Int, `type`: String) {
    println(s"Method visitTypeInsn: opcode: $opcode, `type`: ${`type`}")

  }

  override def visitFieldInsn (opcode: Int, owner: String, name: String, desc: String) {
    println(s"Method visitFieldInsn: opcode: $opcode, owner: $owner, name: $name, desc: $desc")
//    val argTypes = Type.getArgumentTypes(desc)
//    println(argTypes.toList)
  }

  override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String, itf: Boolean) {
    println(s"Method visitMethodInsn: opcode: $opcode, owner: $owner, name: $name, desc: $desc, itf: $itf")
    val argTypes = Type.getArgumentTypes(desc)
    println(argTypes.toList)
  }

  override def visitInvokeDynamicInsn (name: String, desc: String, bsm: Handle, bsmArgs: AnyRef *) {
    println(s"Method visitInvokeDynamicInsn: name: $name, desc: $desc, bsm: $bsm, bsmArgs: $bsmArgs")

  }

  override def visitJumpInsn (opcode: Int, label: Label) {
    println(s"Method visitJumpInsn: opcode: $opcode, label: $label")
  }

  override def visitLdcInsn (cst: AnyRef) {
    println(s"Method visitLdcInsn: cst: $cst")
  }

  override def visitInsn (opcode: Int) {
    println(s"Method visitInsn: opcode: $opcode")
  }

}

class FieldDependencyVisitor extends FieldVisitor(ASM5){


  override def visitAnnotation(
                                desc: String,
                                visible: Boolean): AnnotationVisitor = {
    println(s"Field visitAnnotation: desc: $desc, visible: $visible")
    new AnnotationDependencyVisitor
  }


  override def visitAttribute (attr: Attribute) {
    println(s"Field visitAttribute: attr: $attr")
  }
}

class AnnotationDependencyVisitor extends AnnotationVisitor(ASM5){
  override def visit (name: String, value: AnyRef) {

    println(s"Annotation visit: name: $name, value: $value")
  }

  override def visitAnnotation(
                                name: String,
                                desc: String): AnnotationVisitor = {
    println(s"Annotation visitAnnotation: name: $name, desc: $desc")
    this
  }
}