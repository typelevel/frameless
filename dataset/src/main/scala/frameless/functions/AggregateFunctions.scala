package frameless
package functions

import org.apache.spark.sql.FramelessInternals.expr
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{functions => untyped}

trait AggregateFunctions {
  def lit[T, U: TypedEncoder](value: U): TypedColumn[U] = {
    val encoder = TypedEncoder[U]
    val untyped = Literal.create(value, encoder.sourceDataType)
    new TypedColumn[U](encoder.extractorFor(untyped))
  }

  def count[T](): TypedAggregateAndColumn[Long, Long] = {
    new TypedAggregateAndColumn(untyped.count(untyped.lit(1)))
  }

  def count[T](column: TypedColumn[_]): TypedAggregateAndColumn[Long, Long] = {
    new TypedAggregateAndColumn[Long, Long](untyped.count(column.untyped))
  }

  def sum[A](column: TypedColumn[A])(
    implicit
    summable: CatalystSummable[A],
    encoder: TypedEncoder[A])
  : TypedAggregateAndColumn[A, A] = {
    val zeroExpr = Literal.create(summable.zero, encoder.targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregateAndColumn[A, A](sumOrZero)
  }

  def avg[A: Averageable](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.avg(column.untyped))
  }

  def variance[A: Variance](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.variance(column.untyped))
  }

  def stddev[A: Variance](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.stddev(column.untyped))
  }

  def max[A: Ordering](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.max(column.untyped))
  }

  def min[A: Ordering](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.min(column.untyped))
  }

  def first[A](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.first(column.untyped))
  }

  def last[A](column: TypedColumn[A])(
    implicit
    encoder1: TypedEncoder[A],
    encoder2: TypedEncoder[Option[A]]
  ): TypedAggregateAndColumn[A, Option[A]] = {
    new TypedAggregateAndColumn[A, Option[A]](untyped.last(column.untyped))
  }
}
