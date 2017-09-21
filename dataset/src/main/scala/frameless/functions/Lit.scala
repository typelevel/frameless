package frameless.functions

import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NonSQLExpression}
import org.apache.spark.sql.types.DataType

case class FramelessLit[A](obj: A, encoder: TypedEncoder[A]) extends Expression with NonSQLExpression {
  override def nullable: Boolean = encoder.nullable
  override def toString: String = s"FramelessLit($obj)"

  def eval(input: InternalRow): Any = {
    val ctx = new CodegenContext()
    val eval = genCode(ctx)

    val codeBody = s"""
      public scala.Function1<InternalRow, Object> generate(Object[] references) {
        return new FramelessLitEvalImpl(references);
      }

      class FramelessLitEvalImpl extends scala.runtime.AbstractFunction1<InternalRow, Object> {
        private final Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public FramelessLitEvalImpl(Object[] references) {
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

    val codegen = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[InternalRow => AnyRef]

    codegen(input)
  }

  def dataType: DataType = encoder.catalystRepr
  def children: Seq[Expression] = Nil

  override def genCode(ctx: CodegenContext): ExprCode = {
    encoder.toCatalyst(new Literal(obj, encoder.jvmRepr)).genCode(ctx)
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}
