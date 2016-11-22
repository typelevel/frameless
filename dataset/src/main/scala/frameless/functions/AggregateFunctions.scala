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

  def sum[T, Sel, Agg](column: TypedColumn[T, Sel])(
    implicit
    summable: CatalystSummable[Sel, Agg],
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Agg])
  : TypedAggregateAndColumn[T, Agg, Sel] = {
    val zeroExpr = Literal.create(summable.zero, encoder2.targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregateAndColumn[T, Agg, Sel](sumOrZero)
  }

  def avg[T, Sel, Agg](column: TypedColumn[T, Sel])(
    implicit
    averageable: CatalystAverageable[Sel, Agg],
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Agg]]
  ): TypedAggregateAndColumn[T, Option[Agg], Sel] = {
    new TypedAggregateAndColumn[T, Option[Agg], Sel](untyped.avg(column.untyped))
  }

  def variance[T, Sel, Agg](column: TypedColumn[T, Sel])(
    implicit
    variance: CatalystVariance[Sel, Agg],
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Agg]]
  ): TypedAggregateAndColumn[T, Option[Agg], Sel] = {
    new TypedAggregateAndColumn[T, Option[Agg], Sel](untyped.variance(column.untyped))
  }

  def stddev[T, Sel, Agg](column: TypedColumn[T, Sel])(
    implicit
    variance: CatalystVariance[Sel, Agg],
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Agg]]
  ): TypedAggregateAndColumn[T, Option[Agg], Sel] = {
    new TypedAggregateAndColumn[T, Option[Agg], Sel](untyped.stddev(column.untyped))
  }

  def max[T, Sel: CatalystOrdered](column: TypedColumn[T, Sel])(
    implicit
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Sel]]
  ): TypedAggregateAndColumn[T, Option[Sel], Sel] = {
    new TypedAggregateAndColumn[T, Option[Sel], Sel](untyped.max(column.untyped))
  }

  def min[T, Sel: CatalystOrdered](column: TypedColumn[T, Sel])(
    implicit
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Sel]]
  ): TypedAggregateAndColumn[T, Option[Sel], Sel] = {
    new TypedAggregateAndColumn[T, Option[Sel], Sel](untyped.min(column.untyped))
  }

  def first[T, Sel](column: TypedColumn[T, Sel])(
    implicit
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Sel]]
  ): TypedAggregateAndColumn[T, Option[Sel], Sel] = {
    new TypedAggregateAndColumn[T, Option[Sel], Sel](untyped.first(column.untyped))
  }

  def last[T, Sel](column: TypedColumn[T, Sel])(
    implicit
    encoder1: TypedEncoder[Sel],
    encoder2: TypedEncoder[Option[Sel]]
  ): TypedAggregateAndColumn[T, Option[Sel], Sel] = {
    new TypedAggregateAndColumn[T, Option[Sel], Sel](untyped.last(column.untyped))
  }
}
