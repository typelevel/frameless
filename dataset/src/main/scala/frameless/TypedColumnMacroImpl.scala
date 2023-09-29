package frameless

import scala.reflect.macros.whitebox

private[frameless] object TypedColumnMacroImpl {

  def applyImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](c: whitebox.Context)(x: c.Tree): c.Expr[TypedColumn[T, U]] = {
    import c.universe._

    val t = c.weakTypeOf[T]
    val u = c.weakTypeOf[U]

    def buildExpression(path: List[String]): c.Expr[TypedColumn[T, U]] = {
      val columnName = path.mkString(".")

      c.Expr[TypedColumn[T, U]](q"new _root_.frameless.TypedColumn[$t, $u]((org.apache.spark.sql.functions.col($columnName)).expr)")
    }

    def abort(msg: String) = c.abort(c.enclosingPosition, msg)

    @annotation.tailrec
    def path(in: Select, out: List[TermName]): List[TermName] =
      in.qualifier match {
        case sub: Select =>
          path(sub, in.name.toTermName :: out)

        case id: Ident =>
          id.name.toTermName :: in.name.toTermName :: out

        case u =>
          abort(s"Unsupported selection: $u")
      }

    @annotation.tailrec
    def check(current: Type, in: List[TermName]): Boolean = in match {
      case next :: tail => {
        val sym = current.decl(next).asTerm

        if (!sym.isStable) {
          abort(s"Stable term expected: ${current}.${next}")
        }

        check(sym.info, tail)
      }

      case _ =>
        true
    }

    x match {
      case fn: Function => fn.body match {
        case select: Select if select.name.isTermName =>
          val expectedRoot: Option[String] = fn.vparams match {
            case List(rt) if rt.rhs == EmptyTree =>
              Option.empty[String]

            case List(rt) =>
              Some(rt.toString)

            case u =>
              abort(s"Select expression must have a single parameter: ${u mkString ", "}")
          }

          path(select, List.empty) match {
            case root :: tail if (
              expectedRoot.forall(_ == root) && check(t, tail)) => {
              val colPath = tail.mkString(".")

              c.Expr[TypedColumn[T, U]](q"new _root_.frameless.TypedColumn[$t, $u]((org.apache.spark.sql.functions.col($colPath)).expr)")
            }

            case _ =>
              abort(s"Invalid select expression: $select")
          }

        case t =>
          abort(s"Select expression expected: $t")
      }

      case _ =>
        abort(s"Function expected: $x")
    }
  }
}
