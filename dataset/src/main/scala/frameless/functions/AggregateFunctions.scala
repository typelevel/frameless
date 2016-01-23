package frameless
package functions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum, Count}

trait AggregateFunctions {
  def lit[T, U: TypedEncoder](value: U): TypedColumn[T, U] = {
    val encoder = TypedEncoder[U]
    val untyped = Literal.create(value, encoder.sourceDataType)
    new TypedColumn[T, U](encoder.extractorFor(untyped))
  }

  def count[T](): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](Count(Literal(1)).toAggregateExpression())
  }

  def count[T](column: TypedColumn[T, _]): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](Count(column.expr).toAggregateExpression())
  }

  def sum[A: Summable, T](column: TypedColumn[T, A])(
    implicit encoder: TypedEncoder[Option[A]]
  ): TypedColumn[T, Option[A]] = {
    new TypedColumn[T, Option[A]](Sum(column.expr).toAggregateExpression())
  }

  def sum[A: Summable, T](column: TypedColumn[T, A], default: A)(
    implicit encoder: TypedEncoder[A]
  ): TypedColumn[T, A] = {
    new TypedColumn[T, A](
      Coalesce(List(
        Sum(column.expr).toAggregateExpression(),
        lit(default).expr
      ))
    )
  }

  def avg[A: Averagable, T](column: TypedColumn[T, A])(
    implicit encoder: TypedEncoder[Option[A]]
  ): TypedColumn[T, Option[A]] = {
    new TypedColumn[T, Option[A]](Average(column.expr).toAggregateExpression())
  }
}
