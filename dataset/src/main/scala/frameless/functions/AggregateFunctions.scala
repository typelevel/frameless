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
  def lit[A: TypedEncoder](value: A): TypedColumn[A] = frameless.functions.lit(value)

  /** Aggregate function: returns the number of items in a group.
    *
    * apache/spark
    */
  def count(): TypedAggregate[Long] = {
    new TypedAggregate(untyped.count(untyped.lit(1)))
  }

  /** Aggregate function: returns the number of items in a group for which the selected column is not null.
    *
    * apache/spark
    */
  def count(column: TypedColumn[_]): TypedAggregate[Long] = {
    new TypedAggregate[Long](untyped.count(column.untyped))
  }

  /** Aggregate function: returns the number of distinct items in a group.
    *
    * apache/spark
    */
  def countDistinct(column: TypedColumn[_]): TypedAggregate[Long] = {
    new TypedAggregate[Long](untyped.countDistinct(column.untyped))
  }

  /** Aggregate function: returns the approximate number of distinct items in a group.
    */
  def approxCountDistinct(column: TypedColumn[_]): TypedAggregate[Long] = {
    new TypedAggregate[Long](untyped.approx_count_distinct(column.untyped))
  }

  /** Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @param rsd maximum estimation error allowed (default = 0.05)
    *
    * apache/spark
    */
  def approxCountDistinct(column: TypedColumn[_], rsd: Double): TypedAggregate[Long] = {
    new TypedAggregate[Long](untyped.approx_count_distinct(column.untyped, rsd))
  }

  /** Aggregate function: returns a list of objects with duplicates.
    *
    * apache/spark
    */
  def collectList[A: TypedEncoder](column: TypedColumn[A]): TypedAggregate[Vector[A]] = {
    new TypedAggregate[Vector[A]](untyped.collect_list(column.untyped))
  }

  /** Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * apache/spark
    */
  def collectSet[A: TypedEncoder](column: TypedColumn[A]): TypedAggregate[Vector[A]] = {
    new TypedAggregate[Vector[A]](untyped.collect_set(column.untyped))
  }

  /** Aggregate function: returns the sum of all values in the given column.
    *
    * apache/spark
    */
  def sum[A, Out](column: TypedColumn[A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].targetDataType)
    val sumExpr = expr(untyped.sum(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregate[Out](sumOrZero)
  }

  /** Aggregate function: returns the sum of distinct values in the column.
    *
    * apache/spark
    */
  def sumDistinct[A, Out](column: TypedColumn[A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].targetDataType)
    val sumExpr = expr(untyped.sumDistinct(column.untyped))
    val sumOrZero = Coalesce(Seq(sumExpr, zeroExpr))

    new TypedAggregate[Out](sumOrZero)
  }

  /** Aggregate function: returns the average of the values in a group.
    *
    * apache/spark
    */
  def avg[A, Out](column: TypedColumn[A])(
    implicit
    averageable: CatalystAverageable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[Out] = {
    new TypedAggregate[Out](untyped.avg(column.untyped))
  }


  /** Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @note In Spark variance always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#186]]
    *
    * apache/spark
    */
  def variance[A: CatalystVariance](column: TypedColumn[A]): TypedAggregate[Double] = {
    new TypedAggregate[Double](untyped.variance(column.untyped))
  }

  /** Aggregate function: returns the sample standard deviation.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#155]]
    *
    * apache/spark
    */
  def stddev[A: CatalystVariance](column: TypedColumn[A]): TypedAggregate[Double] = {
    new TypedAggregate[Double](untyped.stddev(column.untyped))
  }

  /**
    * Aggregate function: returns the standard deviation of a column by population.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#L143]]
    *
    *       apache/spark
    */
  def stddevPop[A](column: TypedColumn[A])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column.uencoder

    new TypedAggregate[Option[Double]](
      untyped.stddev_pop(column.cast[Double].untyped)
    )
  }

  /**
    * Aggregate function: returns the standard deviation of a column by sample.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#L160]]
    *
    *       apache/spark
    */
  def stddevSamp[A](column: TypedColumn[A])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column.uencoder

    new TypedAggregate[Option[Double]](
      untyped.stddev_samp(column.cast[Double].untyped)
    )
  }

  /** Aggregate function: returns the maximum value of the column in a group.
    *
    * apache/spark
    */
  def max[A: CatalystOrdered](column: TypedColumn[A]): TypedAggregate[A] = {
    implicit val c = column.uencoder
    new TypedAggregate[A](untyped.max(column.untyped))
  }

  /** Aggregate function: returns the minimum value of the column in a group.
    *
    * apache/spark
    */
  def min[A: CatalystOrdered](column: TypedColumn[A]): TypedAggregate[A] = {
    implicit val c = column.uencoder
    new TypedAggregate[A](untyped.min(column.untyped))
  }

  /** Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * apache/spark
    */
  def first[A](column: TypedColumn[A]): TypedAggregate[A] = {
    implicit val c = column.uencoder
    new TypedAggregate[A](untyped.first(column.untyped))
  }

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * apache/spark
    */
  def last[A](column: TypedColumn[A]): TypedAggregate[A] = {
    implicit val c = column.uencoder
    new TypedAggregate[A](untyped.last(column.untyped))
  }

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @note In Spark corr always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Corr.scala#L95]]
    *
    *       apache/spark
    */
  def corr[A, B](column1: TypedColumn[A], column2: TypedColumn[B])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double],
    evCanBeDoubleB: CatalystCast[B, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column1.uencoder
    implicit val c2 = column2.uencoder

    new TypedAggregate[Option[Double]](
      untyped.corr(column1.cast[Double].untyped, column2.cast[Double].untyped)
    )
  }

  /**
    * Aggregate function: returns the covariance of two collumns.
    *
    * @note In Spark covar_pop always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Covariance.scala#L82]]
    *
    *       apache/spark
    */
  def covarPop[A, B](column1: TypedColumn[A], column2: TypedColumn[B])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double],
    evCanBeDoubleB: CatalystCast[B, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column1.uencoder
    implicit val c2 = column2.uencoder

    new TypedAggregate[Option[Double]](
      untyped.covar_pop(column1.cast[Double].untyped, column2.cast[Double].untyped)
    )
  }

  /**
    * Aggregate function: returns the covariance of two columns.
    *
    * @note In Spark covar_samp always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Covariance.scala#L93]]
    *
    *       apache/spark
    */
  def covarSamp[A, B](column1: TypedColumn[A], column2: TypedColumn[B])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double],
    evCanBeDoubleB: CatalystCast[B, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column1.uencoder
    implicit val c2 = column2.uencoder

    new TypedAggregate[Option[Double]](
      untyped.covar_samp(column1.cast[Double].untyped, column2.cast[Double].untyped)
    )
  }


  /**
    * Aggregate function: returns the kurtosis of a column.
    *
    * @note In Spark kurtosis always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#L220]]
    *
    *       apache/spark
    */
  def kurtosis[A](column: TypedColumn[A])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column.uencoder

    new TypedAggregate[Option[Double]](
      untyped.kurtosis(column.cast[Double].untyped)
    )
  }

  /**
    * Aggregate function: returns the skewness of a column.
    *
    * @note In Spark skewness always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#L200]]
    *
    *       apache/spark
    */
  def skewness[A](column: TypedColumn[A])(
    implicit
    evCanBeDoubleA: CatalystCast[A, Double]
  ): TypedAggregate[Option[Double]] = {
    implicit val c1 = column.uencoder

    new TypedAggregate[Option[Double]](
      untyped.skewness(column.cast[Double].untyped)
    )
  }
}
