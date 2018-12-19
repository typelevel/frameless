package frameless
package functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._, Block._
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.types.DataType
import shapeless.syntax.std.tuple._

/** Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
trait Udf {

  /** Defines a user-defined function of 1 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A, R: TypedEncoder](f: A => R):
    TypedColumn[T, A] => TypedColumn[T, R] = {
    u =>
      val scalaUdf = FramelessUdf(f, List(u), TypedEncoder[R])
      new TypedColumn[T, R](scalaUdf)
  }

  /** Defines a user-defined function of 2 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, R: TypedEncoder](f: (A1,A2) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf = FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
      new TypedColumn[T, R](scalaUdf)
    }

  /** Defines a user-defined function of 3 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, R: TypedEncoder](f: (A1,A2,A3) => R):
  (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf = FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
      new TypedColumn[T, R](scalaUdf)
    }

  /** Defines a user-defined function of 4 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, A4, R: TypedEncoder](f: (A1,A2,A3,A4) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf = FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
      new TypedColumn[T, R](scalaUdf)
    }

  /** Defines a user-defined function of 5 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, A4, A5, R: TypedEncoder](f: (A1,A2,A3,A4,A5) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R] = {
    case us =>
      val scalaUdf = FramelessUdf(f, us.toList[UntypedExpression[T]], TypedEncoder[R])
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
  rencoder: TypedEncoder[R]
) extends Expression with NonSQLExpression {

  override def nullable: Boolean = rencoder.nullable
  override def toString: String = s"FramelessUdf(${children.mkString(", ")})"

  def eval(input: InternalRow): Any = {
    val ctx = new CodegenContext()
    val eval = genCode(ctx)

    val codeBody = s"""
      public scala.Function1<InternalRow, Object> generate(Object[] references) {
        return new FramelessUdfEvalImpl(references);
      }

      class FramelessUdfEvalImpl extends scala.runtime.AbstractFunction1<InternalRow, Object> {
        private final Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public FramelessUdfEvalImpl(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public java.lang.Object apply(java.lang.Object z) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) z;
          ${eval.code}
          return ${eval.isNull} ? ((Object)null) : ((Object)${eval.value});
        }
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val (clazz, _) = CodeGenerator.compile(code)
    val codegen = clazz.generate(ctx.references.toArray).asInstanceOf[InternalRow => AnyRef]

    codegen(input)
  }

  def dataType: DataType = rencoder.catalystRepr

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this

    // save reference to `function` field from `FramelessUdf` to call it later
    val framelessUdfClassName = classOf[FramelessUdf[_, _]].getName
    val funcClassName = s"scala.Function${children.size}"
    val funcExpressionIdx = ctx.references.size - 1
    val funcTerm = ctx.addMutableState(funcClassName, ctx.freshName("udf"),
      v => s"$v = ($funcClassName)((($framelessUdfClassName)references" +
        s"[$funcExpressionIdx]).function());")

    val (argsCode, funcArguments) = encoders.zip(children).map {
      case (encoder, child) =>
        val eval = child.genCode(ctx)
        val codeTpe = CodeGenerator.boxedType(encoder.jvmRepr)
        val argTerm = ctx.freshName("arg")
        val convert = s"${eval.code}\n$codeTpe $argTerm = ${eval.isNull} ? (($codeTpe)null) : (($codeTpe)(${eval.value}));"

        (convert, argTerm)
    }.unzip

    val internalTpe = CodeGenerator.boxedType(rencoder.jvmRepr)
    val internalTerm = ctx.addMutableState(internalTpe, ctx.freshName("internal"))
    val internalNullTerm = ctx.addMutableState("boolean", ctx.freshName("internalNull"))
    val internalExpr = LambdaVariable(internalTerm, internalNullTerm, rencoder.jvmRepr)

    val resultEval = rencoder.toCatalyst(internalExpr).genCode(ctx)

    ev.copy(code = code"""
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
}

object FramelessUdf {
  // Spark needs case class with `children` field to mutate it
  def apply[T, R](
    function: AnyRef,
    cols: Seq[UntypedExpression[T]],
    rencoder: TypedEncoder[R]
  ): FramelessUdf[T, R] = FramelessUdf(
    function = function,
    encoders = cols.map(_.uencoder).toList,
    children = cols.map(x => x.uencoder.fromCatalyst(x.expr)).toList,
    rencoder = rencoder
  )
}
