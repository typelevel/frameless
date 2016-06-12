package frameless

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{FramelessInternals, Column}
import shapeless.ops.record.Selector
import shapeless._
import scala.annotation.implicitNotFound

sealed trait UntypedExpression[T] {
  def expr: Expression
}

sealed class TypedColumn[T, U](
  val expr: Expression
)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] {

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  def untyped: Column = new Column(expr)

  def ===(other: U): TypedColumn[T, Boolean] = {
    new TypedColumn[T, Boolean](untyped === other)
  }

  def ===(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = {
    new TypedColumn[T, Boolean](untyped === other.untyped)
  }
}

sealed trait TypedAggregate[T, A] extends UntypedExpression[T] {
  def expr: Expression
  def aencoder: TypedEncoder[A]
}

sealed class TypedAggregateAndColumn[T, A, U](expr: Expression)(
  implicit
  val aencoder: TypedEncoder[A],
  uencoder: TypedEncoder[U]
) extends TypedColumn[T, U](expr) with TypedAggregate[T, A] {

  def this(column: Column)(implicit aencoder: TypedEncoder[A], uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
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
