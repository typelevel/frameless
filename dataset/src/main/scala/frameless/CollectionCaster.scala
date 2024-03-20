package frameless

import frameless.TypedEncoder.CollectionConversion
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, ObjectType}

case class CollectionCaster[F[_],C[_],Y](child: Expression, conversion: CollectionConversion[F,C,Y]) extends UnaryExpression with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def eval(input: InternalRow): Any = {
    val o = child.eval(input).asInstanceOf[Object]
    o match {
      case col: F[Y] @unchecked =>
        conversion.convert(col)
      case _ => o
    }
  }

  override def dataType: DataType = child.dataType
}

case class SeqCaster[C[X] <: Iterable[X], Y](child: Expression) extends UnaryExpression {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  // eval on interpreted works, fallback on codegen does not, e.g. with ColumnTests.asCol and Vectors, the code generated still has child of type Vector but child eval returns X2, which is not good
  override def eval(input: InternalRow): Any = {
    val o = child.eval(input).asInstanceOf[Object]
    o match {
      case col: Set[Y]@unchecked =>
        col.toSeq
      case _ => o
    }
  }

  def toSeqOr[T](isSet: => T, or: => T): T =
    child.dataType match {
      case ObjectType(cls) if classOf[scala.collection.Set[_]].isAssignableFrom(cls) =>
        isSet
      case t => or
    }

  override def dataType: DataType =
    toSeqOr(ObjectType(classOf[scala.collection.Seq[_]]), child.dataType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c =>
      toSeqOr(s"$c.toSeq()", s"$c")
    )

}