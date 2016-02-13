package frameless

import org.apache.spark.sql.catalyst.expressions._
import shapeless.ops.record.Selector
import shapeless._

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

  @implicitNotFound(msg = "No columns ${K} of type ${V} in ${T}")
  trait ExistsMany[T, K <: HList, V]

  object ExistsMany {
    implicit def deriveCons[T, KH, KT <: HList, V0, V1](
      implicit
      head: Exists[T, KH, V0],
      tail: ExistsMany[V0, KT, V1]
    ): ExistsMany[T, KH :: KT, V1] = new ExistsMany[T, KH :: KT, V1] {}

    implicit def deriveHNil[T, K, V](
      implicit
      head: Exists[T, K, V]
    ): ExistsMany[T, K :: HNil, V] = new ExistsMany[T, K :: HNil, V] {}
  }

  object Exists {
    def apply[T, V](column: Witness)(
      implicit
      exists: Exists[T, column.T, V]
    ): Exists[T, column.T, V] = exists

    implicit def deriveRecord[T, H <: HList, K, V](
      implicit
      lgen: LabelledGeneric.Aux[T, H],
      selector: Selector.Aux[H, K, V]
    ): Exists[T, K, V] = new Exists[T, K, V] {}
  }
}
