package frameless.column

import frameless.{TypedColumn, TypedEncoder, TypedExpressionEncoder}

import scala.collection.immutable.Queue
import scala.reflect.macros.whitebox

class ColumnMacros(val c: whitebox.Context) {
  import c.universe._

  // could be used to reintroduce apply('foo)
  // $COVERAGE-OFF$ Currently unused
  def fromSymbol[A : WeakTypeTag, B : WeakTypeTag](selector: c.Expr[scala.Symbol])(encoder: c.Expr[TypedEncoder[B]]): Tree = {
    val B = weakTypeOf[B].dealias
    val witness = c.typecheck(q"_root_.shapeless.Witness.apply(${selector.tree})")
    c.typecheck(q"${c.prefix}.col[$B]($witness)")
  }
  // $COVERAGE-ON$

  def fromFunction[A : WeakTypeTag, B : WeakTypeTag](selector: c.Expr[A => B])(encoder: c.Expr[TypedEncoder[B]]): Tree = {
    def fail(tree: Tree) = {
      val err =
        s"Could not create a column identifier from $tree - try using _.a.b syntax"
      c.abort(tree.pos, err)
    }

    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias

    val selectorStr = selector.tree match {
      case NameExtractor(str) => str
      case Function(_, body)  => fail(body)
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => fail(other)
      // $COVERAGE-ON$
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

  case class NameExtractor(name: TermName) { Self =>
    def unapply(tree: Tree): Option[Queue[String]] = {
      tree match {
        case Ident(`name`) => Some(Queue.empty)
        case Select(Self(strs), nested) => Some(strs enqueue nested.toString)
        // $COVERAGE-OFF$ - Not sure if this case can ever come up and Encoder will still work
        case Apply(Self(strs), List()) => Some(strs)
        // $COVERAGE-ON$
        case _ => None
      }
    }
  }

  object NameExtractor {
    def unapply(tree: Tree): Option[String] = tree match {
      case Function(List(ValDef(_, name, argTyp, _)), body) => NameExtractor(name).unapply(body).map(_.mkString("."))
      case _ => None
    }
  }
}
