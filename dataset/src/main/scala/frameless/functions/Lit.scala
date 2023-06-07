package frameless.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.types.DataType

private[frameless] case class Lit[T <: AnyVal](
    dataType: DataType,
    nullable: Boolean,
    show: () => String,
    catalystExpr: Expression // must be a generated Expression from a literal TypedEncoder's toCatalyst function
) extends Expression with NonSQLExpression {
  override def toString: String = s"FramelessLit(${show()})"

  lazy val codegen = {
    val ctx = new CodegenContext()
    val eval = genCode(ctx)

    val codeBody =
      s"""
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
    codegen
  }

  def eval(input: InternalRow): Any = codegen(input)
  
  def children: Seq[Expression] = Nil

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = catalystExpr.genCode(ctx)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = this

  // see https://github.com/typelevel/frameless/pull/721#issuecomment-1581137730 for why true and not catalystExpr.foldable (InvokeLike <3.3.1 SPARK-40380)
  override val foldable: Boolean = true
}
