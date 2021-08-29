package frameless.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.types.DataType

private[frameless] case class Lit[T <: AnyVal](
    dataType: DataType,
    nullable: Boolean,
    toCatalyst: CodegenContext => ExprCode,
    show: () => String)
    extends Expression
    with NonSQLExpression {
  override def toString: String = s"FramelessLit(${show()})"

  def eval(input: InternalRow): Any = {
    val ctx = new CodegenContext()
    val eval = genCode(ctx)

    val codeBody = s"""
      public scala.Function1<InternalRow, Object> generate(Object[] references) {
        return new LiteralEvalImpl(references);
      }

      class LiteralEvalImpl extends scala.runtime.AbstractFunction1<InternalRow, Object> {
        private final Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public LiteralEvalImpl(Object[] references) {
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
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    )

    val (clazz, _) = CodeGenerator.compile(code)
    val codegen =
      clazz.generate(ctx.references.toArray).asInstanceOf[InternalRow => AnyRef]

    codegen(input)
  }

  def children: Seq[Expression] = Nil

  override def genCode(ctx: CodegenContext): ExprCode = toCatalyst(ctx)

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}
