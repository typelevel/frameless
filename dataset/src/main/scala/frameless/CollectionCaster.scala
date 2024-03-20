package frameless

import frameless.TypedEncoder.SeqConversion
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

case class CollectionCaster[C[_]](child: Expression, conversion: SeqConversion[C]) extends UnaryExpression with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def eval(input: InternalRow): Any = {
    val o = child.eval(input).asInstanceOf[Object]
    o match {
      case seq: scala.collection.Seq[_] =>
        conversion.convertSeq(seq)
      case set: scala.collection.Set[_] =>
        o
      case _ => o
    }
  }

  override def dataType: DataType = child.dataType
}
