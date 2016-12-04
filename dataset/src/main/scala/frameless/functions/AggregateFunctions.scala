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

  def sum[A, T, Out](column: TypedColumn[T, A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregateAndColumn[T, Out, Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregateAndColumn[T, Out, Out](sumOrZero)
  }

  def avg[A, T, Out](column: TypedColumn[T, A])(
    implicit
    averageable: CatalystAverageable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregateAndColumn[T, Out, Option[Out]] = {
    new TypedAggregateAndColumn[T, Out, Option[Out]](untyped.avg(column.untyped))
  }

  // In Spark variance always returns Double
  // https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#186
  def variance[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, Double, Option[Double]] = {
    new TypedAggregateAndColumn[T, Double, Option[Double]](untyped.variance(column.untyped))
  }

  // In Spark stddev always returns Double
  // https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#155
  def stddev[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, Double, Option[Double]] = {
    new TypedAggregateAndColumn[T, Double, Option[Double]](untyped.stddev(column.untyped))
  }

  def max[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, A, Option[A]] = {
    import column.uencoder
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.max(column.untyped))
  }

  def min[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, A, Option[A]] = {
    import column.uencoder
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.min(column.untyped))
  }

  def first[A, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, A, Option[A]] = {
    import column.uencoder
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.first(column.untyped))
  }

  def last[A, T](column: TypedColumn[T, A]): TypedAggregateAndColumn[T, A, Option[A]] = {
    import column.uencoder
    new TypedAggregateAndColumn[T, A, Option[A]](untyped.last(column.untyped))
  }
}
