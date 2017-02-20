package frameless.macros

import frameless.{TypedColumn, TypedEncoder}
import org.apache.spark.sql.ColumnName

import scala.collection.immutable.Queue
import scala.reflect.macros.{TypecheckException, whitebox}

class ColumnMacros(val c: whitebox.Context) {
  import c.universe._

  private val TypedExpressionEncoder = reify(frameless.TypedExpressionEncoder)
  private val TypedDataset = reify(frameless.TypedDataset)
  private val ColumnName = weakTypeOf[ColumnName]

  private def toColumn[A : WeakTypeTag, B : WeakTypeTag](
    selectorStr: String,
    encoder: c.Expr[TypedEncoder[B]]
  ): Tree = {

    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias

    val typedCol = appliedType(
      weakTypeOf[TypedColumn[_, _]].typeConstructor, A, B
    )


    val datasetCol = c.typecheck(
      q"new $ColumnName($selectorStr).as[$B]($TypedExpressionEncoder.apply[$B]($encoder))"
    )

    c.typecheck(q"new $typedCol($datasetCol)")
  }

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
      case Function(List(ValDef(_, ArgName(argName), argTyp, _)), body) => body match {
        case `argName`(strs) => strs.mkString(".")
        case other => fail(other)
      }
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => fail(other)
      // $COVERAGE-ON$
    }

    toColumn[A, B](selectorStr, encoder)
  }

  def fromExpr[A : WeakTypeTag, B : WeakTypeTag](expr: c.Expr[A => B])(encoder: c.Expr[TypedEncoder[B]]): Tree = {
    def fail(tree: Tree)(reason: String, pos: Position = tree.pos) = c.abort(
      tree.pos,
      s"Could not expand expression $tree$reason"
    )

    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias

    val result = expr.tree match {
      case Function(List(ValDef(_, ArgName(argName), argTyp, _)), body) =>
        val Extractor = ExprExtractor(argName)
        val selectCols = body match {
          case Extractor(extracted) => extracted match {
            // top-level construct; have to unwrap the args
            case Right(Construct(tpe, args)) =>
              Expr.toNamedColumns(args)
            case Right(colExpr) =>
              List(Expr.refold(colExpr))
            case Left(errs) =>
              errs match {
                case (tree, err) :: Nil => fail(tree)(s": $err")
                case multi =>
                  multi foreach {
                    case (tree, err) => c.error(tree.pos, err)
                  }
                  fail(body)(": multiple errors composing projection")
              }
          }

          case other =>
            val o = other
            fail(o)(": could not compose projection")
        }
        val df = q"${c.prefix}.dataset.toDF().select(..$selectCols)"
        val ds = q"$df.as[$B]($TypedExpressionEncoder.apply[$B])"
        val typedDs = q"$TypedDataset.create[$B]($ds)($encoder)"

        typedDs
    }


    c.typecheck(result)
  }

  sealed trait Expr {
    def tpe: Type
  }

  object Expr {
    // zip down to the leaves and construct an expression that gives us a Column
    // this is some kind of hylo-mylo-bananafanafofylo-morphism.
    def refold(expr: Expr): Tree = expr match {
      case LiteralExpr(tpe, tree) => q"org.apache.spark.sql.functions.lit($tree)"
      case Column(tpe, selectorStr) =>
        q"new $ColumnName($selectorStr)"
      // $COVERAGE-OFF$ - Functions can't currently be used (and currently give an error before this is reached)
      case FunctionApplication(tpe, src, fn, args) =>
        c.abort(c.enclosingPosition, "Functions not yet supported in selectExpr")
      // $COVERAGE-ON$
      case BinaryOperator(tpe, lhs, op, rhs) =>
        val lhsTree = refold(lhs)
        val rhsTree = refold(rhs)
        Apply(Select(lhsTree, op), List(rhsTree))
      case UnaryRAOperator(tpe, rhs, op) =>
        val rhsTree = refold(rhs)
        Select(rhsTree, op)
      case Construct(tpe, args) =>
        val argTrees = toNamedColumns(args)
        q"org.apache.spark.sql.functions.struct(..$argTrees)"
      case TrustMeBro(tpe, tree) => tree
    }

    def toNamedColumns(exprMap: Map[String, Expr]): List[Tree] = exprMap.toList.map {
      case (colName, expr) =>
        val colTree = refold(expr)
        q"$colTree.as($colName)"
    }
  }

  case class Construct(tpe: Type, args: Map[String, Expr]) extends Expr                  // Construct tuple or case class
  case class UnaryRAOperator(tpe: Type, rhs: Expr, op: TermName) extends Expr            // Unary right-associative op
  case class BinaryOperator(tpe: Type, lhs: Expr, op: TermName, rhs: Expr) extends Expr  // Binary op
  case class FunctionApplication(tpe: Type, src: Tree, fn: TermName, args: List[Expr]) extends Expr   // SQL function
  case class Column(tpe: Type, selectorStr: String) extends Expr                         // Column of this dataset
  case class LiteralExpr(tpe: Type, tree: Tree) extends Expr                             // Literal expression

  // manually added tree (temporary until functions can be implemented; needed for operator substitution)
  case class TrustMeBro(tpe: Type, tree: Tree) extends Expr

  case class ExprExtractor(Rooted: NameExtractor) {
    private val This = this

    // substitute these operators with functions (or other trees)
    // i.e. two string columns can't be + together; they must be concat(a, b) instead.
    val subOperators: Map[MethodSymbol, (Tree, Tree) => Tree] = Map(
      weakTypeOf[String].member(TermName("+").encodedName).asMethod ->
        ((lhs, rhs) => q"org.apache.spark.sql.functions.concat($lhs, $rhs)")
    )

    def unapply(tree: Tree): Option[Either[List[(Tree, String)], Expr]] = tree match {

      // A single column with no expression around it
      case Rooted(strs) if tree.symbol.isMethod && tree.symbol.asMethod.isCaseAccessor =>
        Some(Right(Column(tree.tpe, strs.mkString("."))))

      // A unary operator - the operator must exist on org.apache.spark.sql.Column
      case Select(This(rhsE), op: TermName) => Some {
        if(isColumnOp(op, 0)) {
          rhsE.right.map {
            rhs => UnaryRAOperator(tree.tpe, rhs, op)
          }
        } else addError(rhsE)(tree, s"${op.decodedName} is not a valid column operator")
      }

      // A literal constant (would it be useful to distinguish this from non-constant literal?
      case Literal(_) => Some(Right(LiteralExpr(tree.tpe, tree)))

      // Constructing a case class
      case Apply(sel @ Select(qualifier, TermName("apply")), AllExprs(argsE)) => Some {
        if (isConstructor(sel, tree.tpe)) {
          argsE.right.map {
            args =>
              val params = sel.symbol.asMethod.paramLists.head
              val names = sel.symbol.asMethod.paramLists.head.zip(args).map {
                case (param, paramExpr) => param.name.toString -> paramExpr
              }.toMap
              Construct(tree.tpe, names)
          }
        } else addError(argsE)(tree, "Only constructor can be used here")
      }
      // Constructing a tuple or parameterized case class
      case Apply(ta @ TypeApply(sel @ Select(qualifier, TermName("apply")), typArgs), AllExprs(argsE)) => Some {
        if(isConstructor(ta, tree.tpe)) {
          argsE.right.map {
            args =>
              val params = sel.symbol.asMethod.paramLists.head
              val names = sel.symbol.asMethod.paramLists.head.zip(args).map {
                case (param, paramExpr) => param.name.toString -> paramExpr
              }.toMap
              Construct(tree.tpe, names)
          }
        } else addError(argsE)(tree, "Only constructor can be used here")
      }

      // A binary operator - the operator must exist on org.apache.spark.sql.Column
      case Apply(sel @ Select(This(lhsE), op: TermName), List(This(rhsE))) => Some {
        if (isColumnOp(op, 1)) {
          (lhsE, rhsE) match {
            case (Right(lhs), Right(rhs)) => Right {
              subOperators.get(sel.symbol.asMethod).map {
                sub =>
                  val trusted = sub(Expr.refold(lhs), Expr.refold(rhs))
                  TrustMeBro(tree.tpe, trusted)
              }.getOrElse {
                BinaryOperator(tree.tpe, lhs, op, rhs)
              }
            }
            case (lhs, rhs) => failFrom(lhs, rhs)
          }
        } else addError(lhsE, rhsE)(tree, s"${op.decodedName} is not a valid column operator")
      }

      // A function application - what to do with this? How can we check if it's an OK function?
      // Check org.apache.spark.sql.functions?
      // I think we have to port all spark functions to typed versions so we can typecheck here
      case Apply(Select(src, fn: TermName), AllExprs(argsE)) =>
        Some(Left(List((tree, "Function application not currently supported"))))
        //Some(argsE.right.map(args => FunctionApplication(tree.tpe, src, fn, args)))

      case Apply(_, _) => Some(Left(List((tree, "Function application not currently supported"))))

      case _ => None
    }

    private def isColumnOp(name: TermName, numArgs: Int): Boolean = {
      val sym = weakTypeOf[org.apache.spark.sql.Column].member(name)
      if(sym.isMethod) {
        val bothEmptyArgs = numArgs == 0 && sym.asMethod.paramLists.isEmpty
        val matchingArgCounts = sym.asMethod.paramLists.map(_.length) == List(numArgs)
        if (bothEmptyArgs || matchingArgCounts)
          true
        else
          false
      } else {
        false
      }
    }

    def isConstructor(tree: Tree, result: Type, appliedTypes: Option[List[Type]] = None): Boolean = {
      val (qualifier, typeArgs) = tree match {
        case TypeApply(Select(q, _), args) => (q, args)
        case Select(q, _) => (q, Nil)
      }

      val MethodType(params, _) = tree.tpe

      val companion = Option(result.companion).filterNot(_ == NoType).getOrElse {
        try {
          c.typecheck(Ident(result.typeSymbol.name.toTermName)).tpe
        } catch {
          case TypecheckException(_, _) => NoType
        }
      }

      if(tree.symbol.isMethod && companion =:= qualifier.tpe) {
        // must have same args as primary constructor
        val meth = tree.symbol.asMethod
        result.members.find(s => s.isConstructor && s.asMethod.isPrimaryConstructor) match {
          case Some(defaultConstructor) =>
            defaultConstructor.asMethod.paramLists.head.zip(params).forall {
              case (arg1, arg2) =>
                val sameArg = arg1.name == arg2.name
                val arg1Typ = result.member(arg1.asTerm.name).typeSignatureIn(result).finalResultType
                val sameType = arg1Typ =:= arg2.typeSignature
                sameArg && sameType
            }
          case _ => false
        }
      } else false
    }

    object AllExprs {
      def unapply(trees: List[Tree]): Option[Either[List[(Tree, String)], List[Expr]]] = {
        val eithersOpt = trees.map(This.unapply).foldRight[Option[List[Either[(Tree, String), Expr]]]](Some(Nil)) {
          (nextOpt, accumOpt) => for {
            next  <- nextOpt
            accum <- accumOpt
          } yield next match {
            case Left(errs)  => errs.map(Left(_)) ::: accum
            case Right(expr) => Right(expr) :: accum
          }
        }

        // wish cats was here
        eithersOpt.map {
          eithers =>
            eithers.foldRight[Either[List[(Tree, String)], List[Expr]]](Right(Nil)) {
              (next, accum) => next.fold(
                err  => Left(err :: accum.left.toOption.getOrElse(Nil)),
                expr => accum.right.map(expr :: _)
              )
            }
        }
      }
    }
  }

  def addError[A](
    exprs: Either[List[(Tree, String)], A]*)(
    tree: Tree, err: String
  ): Either[List[(Tree, String)], Expr] = failFrom(exprs: _*).left.map(_ :+ (tree -> err))

  def failFrom[A](exprs: Either[List[(Tree, String)], A]*): Left[List[(Tree, String)], Expr] = Left {
    exprs.foldLeft[List[(Tree, String)]](Nil) {
      (accum, next) => next.fold(
        errs => accum ::: errs,
        _    => accum
      )
    }
  }

  case class NameExtractor(name: TermName) {
    private val This = this
    def unapply(tree: Tree): Option[Queue[String]] = {
      tree match {
        case Ident(`name`) => Some(Queue.empty)
        case Select(This(strs), nested) => Some(strs enqueue nested.toString)
        case _ => None
      }
    }
  }

  object ArgName {
    def unapply(name: TermName): Option[NameExtractor] = Some(NameExtractor(name))
  }
}
