package frameless.column

import frameless.{TypedColumn, TypedEncoder, TypedExpressionEncoder}

import scala.collection.immutable.Queue
import scala.reflect.macros.whitebox

class ColumnMacros(val c: whitebox.Context) {
  import c.universe._

  // could be used to reintroduce apply('foo)
  def fromSymbol[A : WeakTypeTag, B : WeakTypeTag](selector: c.Expr[scala.Symbol])(encoder: c.Expr[TypedEncoder[B]]): Tree = {
    val B = weakTypeOf[B].dealias
    val witness = c.typecheck(q"_root_.shapeless.Witness.apply(${selector.tree})")
    c.typecheck(q"${c.prefix}.col[$B]($witness)")
  }

  def fromFunction[A : WeakTypeTag, B : WeakTypeTag](selector: c.Expr[A => B])(encoder: c.Expr[TypedEncoder[B]]): Tree = {
    def fail(tree: Tree) = c.abort(
      tree.pos,
      s"Could not create a column identifier from $tree - try using _.a.b syntax")

    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias

    val selectorStr = selector.tree match {
      case Function(List(ValDef(_, ArgName(argName), argTyp, _)), body) => body match {
        case `argName`(strs) => strs.mkString(".")
        case other => fail(other)
      }
      case other => fail(other)
    }

    val typedCol = appliedType(
      weakTypeOf[TypedColumn[_, _]].typeConstructor, A, B
    )

    val TEEObj = reify(TypedExpressionEncoder)

    val datasetCol = c.typecheck(
      q"${c.prefix}.dataset.col($selectorStr).as[$B]($TEEObj.apply[$B]($encoder))"
    )

    c.typecheck(q"new $typedCol($datasetCol)")
  }

  case class NameExtractor(name: TermName) {
    private val This = this
    def unapply(tree: Tree): Option[Queue[String]] = {
      tree match {
        case Ident(`name`) => Some(Queue.empty)
        case Select(This(strs), nested) => Some(strs enqueue nested.toString)
        case Apply(This(strs), List()) => Some(strs)
        case _ => None
      }
    }
  }

  object ArgName {
    def unapply(name: TermName): Option[NameExtractor] = Some(NameExtractor(name))
  }
}
