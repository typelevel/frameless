package frameless
package functions

import org.apache.spark.sql.FramelessInternals.expr
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{functions => untyped}

trait AggregateFunctions {

  /** Creates a [[frameless.TypedColumn]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    */
  def lit[A: TypedEncoder, T](value: A): TypedColumn[T, A] = frameless.functions.lit(value)

  /** Aggregate function: returns the number of items in a group.
    *
    * apache/spark
    */
  def count[T](): TypedAggregate[T, Long] = {
    new TypedAggregate(untyped.count(untyped.lit(1)))
  }

  /** Aggregate function: returns the number of items in a group for which the selected column is not null.
    *
    * apache/spark
    */
  def count[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] = {
    new TypedAggregate[T, Long](untyped.count(column.untyped))
  }

  /** Aggregate function: returns the number of distinct items in a group.
    *
    * apache/spark
    */
  def countDistinct[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] = {
    new TypedAggregate[T, Long](untyped.countDistinct(column.untyped))
  }

  /** Aggregate function: returns the approximate number of distinct items in a group.
    */
  def approxCountDistinct[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] = {
    new TypedAggregate[T, Long](untyped.approxCountDistinct(column.untyped))
  }

  /** Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @param rsd maximum estimation error allowed (default = 0.05)
    *
    * apache/spark
    */
  def approxCountDistinct[T](column: TypedColumn[T, _], rsd: Double): TypedAggregate[T, Long] = {
    new TypedAggregate[T, Long](untyped.approxCountDistinct(column.untyped, rsd))
  }

  /** Aggregate function: returns a list of objects with duplicates.
    *
    * apache/spark
    */
  def collectList[T, A: TypedEncoder](column: TypedColumn[T, A]): TypedAggregate[T, Vector[A]] = {
    new TypedAggregate[T, Vector[A]](untyped.collect_list(column.untyped))
  }

  /** Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * apache/spark
    */
  def collectSet[T, A: TypedEncoder](column: TypedColumn[T, A]): TypedAggregate[T, Vector[A]] = {
    new TypedAggregate[T, Vector[A]](untyped.collect_set(column.untyped))
  }

  /** Aggregate function: returns the sum of all values in the given column.
    *
    * apache/spark
    */
  def sum[A, T, Out](column: TypedColumn[T, A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[T, Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregate[T, Out](sumOrZero)
  }

  /** Aggregate function: returns the sum of distinct values in the column.
    *
    * apache/spark
    */
  def sumDistinct[A, T, Out](column: TypedColumn[T, A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[T, Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].targetDataType)
    val sumExpr = expr(untyped.sumDistinct(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregate[T, Out](sumOrZero)
  }

  /** Aggregate function: returns the average of the values in a group.
    *
    * apache/spark
    */
  def avg[A, T, Out](column: TypedColumn[T, A])(
    implicit
    averageable: CatalystAverageable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[T, Out] = {
    new TypedAggregate[T, Out](untyped.avg(column.untyped))
  }


  /** Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @note In Spark variance always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#186]]
    *
    * apache/spark
    */
  def variance[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregate[T, Double] = {
    new TypedAggregate[T, Double](untyped.variance(column.untyped))
  }

  /** Aggregate function: returns the sample standard deviation.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#155]]
    *
    * apache/spark
    */
  def stddev[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregate[T, Double] = {
    new TypedAggregate[T, Double](untyped.stddev(column.untyped))
  }

  /** Aggregate function: returns the maximum value of the column in a group.
    *
    * apache/spark
    */
  def max[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    new TypedAggregate[T, A](untyped.max(column.untyped))
  }

  /** Aggregate function: returns the minimum value of the column in a group.
    *
    * apache/spark
    */
  def min[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    new TypedAggregate[T, A](untyped.min(column.untyped))
  }

  /** Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * apache/spark
    */
  def first[A, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    new TypedAggregate[T, A](untyped.first(column.untyped))
  }

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * apache/spark
    */
  def last[A, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    new TypedAggregate[T, A](untyped.last(column.untyped))
  }

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @note In Spark corr always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Corr.scala#L95]]
    *
    *       apache/spark
    */
  def corr[A, B, T](column1: TypedColumn[T, A], column2: TypedColumn[T, B])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double],
    evCanBeDoubleB: CatalystCast[B, Double]
  ): TypedAggregate[T, Option[Double]] = {
    implicit val c1 = column1.uencoder
    implicit val c2 = column2.uencoder

    new TypedAggregate[T, Option[Double]](
      untyped.corr(column1.cast[Double].untyped, column2.cast[Double].untyped)
    )
  }
}