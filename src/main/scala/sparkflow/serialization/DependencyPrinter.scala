package sparkflow.serialization

import org.objectweb.asm._
import org.objectweb.asm.Opcodes._

/**
  * Created by ngoehausen on 4/11/16.
  */
object DependencyPrinter {




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


}
