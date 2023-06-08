package frameless

import org.apache.spark.sql.FramelessSpark32Internals
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Coalesce, Expression, If, NaNvl, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.InvokeLike
import org.apache.spark.sql.types.{DataType, ObjectType}

object FoldableImpl {

  trait ExpressionProxy {

    def child: Expression

    protected def withNewChildInternal(newChild: Expression): Expression = ???

    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

    def dataType: DataType = child.dataType

  }

  // backported from SPARK-40380
  case class InvokeLikeImpl(child: InvokeLike) extends UnaryExpression with ExpressionProxy {
    // Returns true if we can trust all values of the given DataType can be serialized.
    def trustedSerializable(dt: DataType): Boolean = {
      // Right now we conservatively block all ObjectType (Java objects) regardless of
      // serializability, because the type-level info with java.io.Serializable and
      // java.io.Externalizable marker interfaces are not strong guarantees.
      // This restriction can be relaxed in the future to expose more optimizations.
      !FramelessSpark32Internals.existsRecursively(dt)(_.isInstanceOf[ObjectType])
    }

    override def foldable =
      child.children.forall(_.foldable) && deterministic && trustedSerializable(dataType)
  }

  // foldable not implemented in 3.2, is in 3.3 (SPARK-39106)
  case class ConditionalExpressionImpl(child: Expression) extends UnaryExpression with ExpressionProxy {
    override def foldable =
      child.children.forall(_.foldable)
  }

  // needed as we cannot test foldable on any parent expression if they have Invoke
  // but similarly we cannot assume the parent is foldable - so we replace InvokeLike
  def replaced(expression: Expression): Expression = expression transformUp {
    case il: InvokeLike => InvokeLikeImpl(il)
    case e@( _: If | _: CaseWhen | _: Coalesce | _: NaNvl ) =>
      ConditionalExpressionImpl(e)
  }

  def foldableCompat(expression: Expression): Boolean =
    replaced(expression).foldable
}
