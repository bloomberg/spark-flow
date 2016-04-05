package sparkflow.serialization

import reflect.macros.whitebox.Context
import language.experimental.macros
import scala.tools.reflect.ToolBox

object FindFreeVars {

  /**
    * Our find macro - just calls helper functions.
    */
  def findMacro
  (c: Context)(func: c.Tree): c.Expr[List[(String, Any)]] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(func)
    c.Expr[List[(String, Any)]](q"invokeAndReturn($func, $closedVars)")
  }

  /**
    * Our pass macro - just calls helper functions with the param
    */
  def execMacro[T, V]
  (c: Context)(param: c.Tree)(func: c.Tree): c.Expr[V] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(func)
    println("Exec macro vars:")
    closedVars.foreach(println(_))
    c.Expr[V](q"execAndReturn($func, $param, $closedVars)")
  }

  /**
    * Generates a list using quasiquotes from free variables from
    * within a tree
    */
  private def createFreeVariableList(c: Context)(func: c.Tree) = {
    import c.universe._

    val freeVars = findFreeVariabels(c)(func)

    freeVars.map(x => {
      val name = Literal(Constant(x._2))
      val value = x._1
      q"($name, $value)"
    })
  }

  /**
    * Finds free variables within an anonymous function, which
    * are bound from an outer scope.
    *
    * Static or class variables are not found.
    */
  private def findFreeVariabels(c: Context)(func: c.Tree) = {
    import c.universe._

    //Symbol of our function.
    def targetSymbol = c.macroApplication.symbol

    /**
      * A traverser which extracts all ValDef nodes from the AST,
      * which are ancestors of the node which hast the symbol targetSymbol.
      */
    class ParentValDefExtractor(targetSymbol: c.Symbol) extends Traverser {
      var defs = List[(String, TreeApi)]()
      var found = false

      //Traverse each child tree, remember wheter we already found
      //our target symbol.
      def traverseChildTrees(trees: List[Tree], include: Boolean): Boolean = {

        var found = false;

        trees.foreach((subtree) => {
          found = found | traverseChildTree(subtree, include)
        })

        found
      }

      //Traverse a single child tree.
      //If the child tree, contains our target, we remember all
      //ValDefs from the child tree and mark this node as ancestor too.
      def traverseChildTree(tree: Tree, include: Boolean): Boolean = {
        val recursiveTraverser = new ParentValDefExtractor(targetSymbol)
        recursiveTraverser.traverse(tree)

        if(recursiveTraverser.found || include) {
          this.defs = recursiveTraverser.defs ::: this.defs
        }

        if(recursiveTraverser.found) {
          this.found = true
        }
        recursiveTraverser.found
      }

      //Traverse the current tree.
      //Check whether we found the target. If so, stop traversion.
      //If not, extract all relevant child trees.
      override def traverse(tree: Tree): Unit = {

        if(targetSymbol == tree.symbol) {
          found = true
        }

        tree match {
          case expr @ ValDef(_, name, _, subtree) =>
            //We fund a val def.
            defs = (name.toString(), expr) :: defs
            super.traverse(subtree)
          case expr @ Bind(name, _) =>
            //We found a bind from a case/match. This is also important to
            //remember.
            defs = (name.toString(), expr) :: defs
          case Block(trees, tree) => traverseChildTrees(tree :: trees, false)
          case Function(params, subtree) => {
            //Special case: If our target is in the subtree
            //of a function call, we also have to include the
            //params of our function in the case.
            traverseChildTrees(params, traverseChildTree(subtree, false))
          }
          case CaseDef(valdef, _, matchexpr) => {
            //Special case: Pattern matching. Handle it similar as function.
            traverseChildTree(valdef, traverseChildTree(matchexpr, false))
          }
          case _ => super.traverse(tree)
        }
      }
    }

    /**
      * Traverser which simply extracts all Ident nodes
      * from a tree.
      */
    class IdentTermExtractor() extends Traverser {
      var idents = List[(Tree, String)]()

      override def traverse(tree: Tree): Unit = tree match {
        case ident @ Ident(name) if !ident.symbol.isMethod =>
          idents = (tree, name.toString) :: idents
        case _ => super.traverse(tree)
      }
    }

    //Extract all Idents from our function
    var termExtractor = new IdentTermExtractor()
    termExtractor.traverse(func)

    //Only keep each symbol once, also filter out packes and so on.
    val filteredTerms = termExtractor.idents.filter(x => {
      !x._1.symbol.isPackage &&
        !x._1.symbol.isMethod &&
        !x._1.symbol.isModule &&
        !x._1.symbol.isClass &&
        !x._1.symbol.isType &&
        x._2 != "_" //Exclude blank.
    })

    //Check if all instances of term are really free
    var distinctFreeTerms = filteredTerms.filter((x) => {
      //For each ident, look for a parent ValDef in our own function.
      val defExtractor = new ParentValDefExtractor(x._1.symbol)
      defExtractor.traverse(func)

      //If we define this val ourself, drop it.
      val defs = defExtractor.defs
      defs.find(y => x._2 == y._1).isEmpty
    }).groupBy(x => x._2).map(x => x._2.head).toList

    distinctFreeTerms
  }
}