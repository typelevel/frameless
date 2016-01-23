package frameless

import org.apache.spark.sql.catalyst.expressions.{Literal, EqualTo, Expression}
import shapeless.ops.record.Selector
import shapeless.{LabelledGeneric, HList, Witness}

import scala.annotation.implicitNotFound

sealed trait UntypedColumn[T] {
  def expr: Expression
}

class TypedColumn[T, U](
  val expr: Expression
)(implicit
  val encoder: TypedEncoder[U]
) extends UntypedColumn[T] {

  def ===(other: U): TypedColumn[T, Boolean] = {
    val newExpr = EqualTo(expr, encoder.extractorFor(Literal(other)))
    new TypedColumn[T, Boolean](newExpr)
  }

  def ===(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = {
    val newExpr = EqualTo(expr, other.expr)
    new TypedColumn[T, Boolean](newExpr)
  }
}

object TypedColumn {
  /**
    * Evidence that type `T` has column `K` with type `V`.
    */
  @implicitNotFound(msg = "No column ${K} of type ${V} in ${T}")
  trait Exists[T, K, V]

  object Exists {
    def apply[T, V](column: Witness)(
      implicit
      ordinal: Exists[T, column.T, V]
    ): Exists[T, column.T, V] = ordinal

    implicit def deriveRecord[T, H <: HList, K, V](
      implicit
      lgen: LabelledGeneric.Aux[T, H],
      selector: Selector.Aux[H, K, V]
    ): Exists[T, K, V] = new Exists[T, K, V] {}
  }
}
