package frameless
package functions

import org.apache.spark.sql.FramelessInternals.expr
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{functions => untyped}

trait AggregateFunctions {
  def lit[T, U: TypedEncoder](value: U): TypedColumn[T, U] = {
    val encoder = TypedEncoder[U]
    val untyped = Literal.create(value, encoder.sourceDataType)
    new TypedColumn[T, U](encoder.extractorFor(untyped))
  }

  def count[T](): TypedAggregateAndColumn[T, Long, Long] = {
    new TypedAggregateAndColumn(untyped.count(untyped.lit(1)))
  }

  def count[T](column: TypedColumn[T, _]): TypedAggregateAndColumn[T, Long, Long] = {
    new TypedAggregateAndColumn[T, Long, Long](untyped.count(column.untyped))
  }

  def sum[A, T](column: TypedColumn[T, A])(
    implicit
    summable: Summable[A],
    encoder: TypedEncoder[A])
  : TypedAggregateAndColumn[T, A, A] = {
    val zeroExpr = Literal.create(summable.zero, encoder.targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregateAndColumn[T, A, A](sumOrZero)
  }

  def avg[A: Averageable, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.avg(column.untyped))
  }

  def variance[A: Variance, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.variance(column.untyped))
  }

  def stddev[A: Variance, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.stddev(column.untyped))
  }

  def max[A: Ordering, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.max(column.untyped))
  }

  def min[A: Ordering, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.min(column.untyped))
  }

  def first[A, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.first(column.untyped))
  }

  def last[A, T](column: TypedColumn[T, A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[T, A, Option[A]] = {
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.last(column.untyped))
  }
}
