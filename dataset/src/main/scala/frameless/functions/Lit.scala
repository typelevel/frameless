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

  def eval(input: InternalRow): Any = catalystExpr.eval(input)

  def children: Seq[Expression] = Nil

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = catalystExpr.genCode(ctx)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = this

  override val foldable: Boolean = true
}
