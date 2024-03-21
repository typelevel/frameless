package frameless
package functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  LeafExpression,
  NonSQLExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen._
import Block._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.DataType
import shapeless.syntax.std.tuple._

/**
 * Documentation marked "apache/spark" is thanks to apache/spark Contributors
 * at https://github.com/apache/spark, licensed under Apache v2.0 available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
trait Udf {

  /**
   * Defines a user-defined function of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * apache/spark
   */
  def udf[T, A, R: TypedEncoder](f: A => R): TypedColumn[T, A] => TypedColumn[T, R] = {
    u =>
      val scalaUdf = FramelessUdf(
        f,
        List(u),
        TypedEncoder[R],
        s => f(s.head.asInstanceOf[A])
      )
      new TypedColumn[T, R](scalaUdf)
  }

  /**
   * Defines a user-defined function of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * apache/spark
   */
  def udf[T, A1, A2, R: TypedEncoder](f: (A1, A2) => R): (
      TypedColumn[T, A1],
      TypedColumn[T, A2]
    ) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf =
<<<<<<< HEAD
        FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
=======
        FramelessUdf(
          f,
          us.toList[UntypedExpression[T]],
          TypedEncoder[R],
          s => f(s.head.asInstanceOf[A1], s(1).asInstanceOf[A2])
        )
>>>>>>> 3bdb8ad (#803 - clean udf from #804, no shim start)
      new TypedColumn[T, R](scalaUdf)
  }

  /**
   * Defines a user-defined function of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * apache/spark
   */
  def udf[T, A1, A2, A3, R: TypedEncoder](f: (A1, A2, A3) => R): (
      TypedColumn[T, A1],
      TypedColumn[T, A2],
      TypedColumn[T, A3]
    ) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf =
<<<<<<< HEAD
        FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
=======
        FramelessUdf(
          f,
          us.toList[UntypedExpression[T]],
          TypedEncoder[R],
          s =>
            f(
              s.head.asInstanceOf[A1],
              s(1).asInstanceOf[A2],
              s(2).asInstanceOf[A3]
            )
        )
>>>>>>> 3bdb8ad (#803 - clean udf from #804, no shim start)
      new TypedColumn[T, R](scalaUdf)
  }

  /**
   * Defines a user-defined function of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * apache/spark
   */
  def udf[T, A1, A2, A3, A4, R: TypedEncoder](f: (A1, A2, A3, A4) => R): (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf =
<<<<<<< HEAD
        FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
=======
        FramelessUdf(
          f,
          us.toList[UntypedExpression[T]],
          TypedEncoder[R],
          s =>
            f(
              s.head.asInstanceOf[A1],
              s(1).asInstanceOf[A2],
              s(2).asInstanceOf[A3],
              s(3).asInstanceOf[A4]
            )
        )
>>>>>>> 3bdb8ad (#803 - clean udf from #804, no shim start)
      new TypedColumn[T, R](scalaUdf)
  }

  /**
   * Defines a user-defined function of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * apache/spark
   */
  def udf[T, A1, A2, A3, A4, A5, R: TypedEncoder](f: (A1, A2, A3, A4, A5) => R): (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf =
<<<<<<< HEAD
        FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
=======
        FramelessUdf(
          f,
          us.toList[UntypedExpression[T]],
          TypedEncoder[R],
          s =>
            f(
              s.head.asInstanceOf[A1],
              s(1).asInstanceOf[A2],
              s(2).asInstanceOf[A3],
              s(3).asInstanceOf[A4],
              s(4).asInstanceOf[A5]
            )
        )
>>>>>>> 3bdb8ad (#803 - clean udf from #804, no shim start)
      new TypedColumn[T, R](scalaUdf)
  }
}

/**
 * NB: Implementation detail, isn't intended to be directly used.
 *
 * Our own implementation of `ScalaUDF` from Catalyst compatible with [[TypedEncoder]].
 */
case class FramelessUdf[T, R](
    function: AnyRef,
    encoders: Seq[TypedEncoder[_]],
    children: Seq[Expression],
    rencoder: TypedEncoder[R],
    evalFunction: Seq[Any] => Any)
    extends Expression
    with NonSQLExpression {

  override def nullable: Boolean = rencoder.nullable

  override def toString: String = s"FramelessUdf(${children.mkString(", ")})"

  lazy val typedEnc =
    TypedExpressionEncoder[R](rencoder).asInstanceOf[ExpressionEncoder[R]]

  def eval(input: InternalRow): Any = {
    val jvmTypes = children.map(_.eval(input))

    val returnJvm = evalFunction(jvmTypes).asInstanceOf[R]

    val returnCatalyst = typedEnc.createSerializer().apply(returnJvm)
    val retval =
      if (returnCatalyst == null)
        null
      else if (typedEnc.isSerializedAsStructForTopLevel)
        returnCatalyst
      else
        returnCatalyst.get(0, dataType)

    retval
  }

  def dataType: DataType = rencoder.catalystRepr

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this

    // save reference to `function` field from `FramelessUdf` to call it later
    val framelessUdfClassName = classOf[FramelessUdf[_, _]].getName
    val funcClassName = s"scala.Function${children.size}"
    val funcExpressionIdx = ctx.references.size - 1
    val funcTerm = ctx.addMutableState(
      funcClassName,
      ctx.freshName("udf"),
      v =>
        s"$v = ($funcClassName)((($framelessUdfClassName)references" +
          s"[$funcExpressionIdx]).function());"
    )

    val (argsCode, funcArguments) = encoders
      .zip(children)
      .map {
        case (encoder, child) =>
          val eval = child.genCode(ctx)
          val codeTpe = CodeGenerator.boxedType(encoder.jvmRepr)
          val argTerm = ctx.freshName("arg")
          val convert =
            s"${eval.code}\n$codeTpe $argTerm = ${eval.isNull} ? (($codeTpe)null) : (($codeTpe)(${eval.value}));"

          (convert, argTerm)
      }
      .unzip

    val internalTpe = CodeGenerator.boxedType(rencoder.jvmRepr)
    val internalTerm =
      ctx.addMutableState(internalTpe, ctx.freshName("internal"))
    val internalNullTerm =
      ctx.addMutableState("boolean", ctx.freshName("internalNull"))
    // CTw - can't inject the term, may have to duplicate old code for parity
    val internalExpr = Spark2_4_LambdaVariable(
      internalTerm,
      internalNullTerm,
      rencoder.jvmRepr,
      true
    )

    val resultEval = rencoder.toCatalyst(internalExpr).genCode(ctx)

    ev.copy(
      code = code"""
      ${argsCode.mkString("\n")}

      $internalTerm =
        ($internalTpe)$funcTerm.apply(${funcArguments.mkString(", ")});
      $internalNullTerm = $internalTerm == null;

      ${resultEval.code}
      """,
      value = resultEval.value,
      isNull = resultEval.isNull
    )
  }

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]
    ): Expression = copy(children = newChildren)
}

case class Spark2_4_LambdaVariable(
    value: String,
    isNull: String,
    dataType: DataType,
    nullable: Boolean = true)
    extends LeafExpression
    with NonSQLExpression {

  private val accessor: (InternalRow, Int) => Any =
    InternalRow.getAccessor(dataType)

  // Interpreted execution of `LambdaVariable` always get the 0-index element from input row.
  override def eval(input: InternalRow): Any = {
    assert(
      input.numFields == 1,
      "The input row of interpreted LambdaVariable should have only 1 field."
    )
    if (nullable && input.isNullAt(0)) {
      null
    } else {
      accessor(input, 0)
    }
  }

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
    ): ExprCode = {
    val isNullValue = if (nullable) {
      JavaCode.isNullVariable(isNull)
    } else {
      FalseLiteral
    }
    ExprCode(value = JavaCode.variable(value, dataType), isNull = isNullValue)
  }
}

object FramelessUdf {

  // Spark needs case class with `children` field to mutate it
  def apply[T, R](
      function: AnyRef,
      cols: Seq[UntypedExpression[T]],
      rencoder: TypedEncoder[R],
      evalFunction: Seq[Any] => Any
    ): FramelessUdf[T, R] = FramelessUdf(
    function = function,
    encoders = cols.map(_.uencoder).toList,
    children = cols.map(x => x.uencoder.fromCatalyst(x.expr)).toList,
    rencoder = rencoder,
    evalFunction = evalFunction
  )
}
