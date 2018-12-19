package frameless
package functions

import org.apache.spark.sql.FramelessInternals.expr
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{functions => sparkFunctions}
import frameless.syntax._

trait AggregateFunctions {
  /** Aggregate function: returns the number of items in a group.
    *
    * apache/spark
    */
  def count[T](): TypedAggregate[T, Long] =
    sparkFunctions.count(sparkFunctions.lit(1)).typedAggregate

  /** Aggregate function: returns the number of items in a group for which the selected column is not null.
    *
    * apache/spark
    */
  def count[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] =
    sparkFunctions.count(column.untyped).typedAggregate

  /** Aggregate function: returns the number of distinct items in a group.
    *
    * apache/spark
    */
  def countDistinct[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] =
    sparkFunctions.countDistinct(column.untyped).typedAggregate

  /** Aggregate function: returns the approximate number of distinct items in a group.
    */
  def approxCountDistinct[T](column: TypedColumn[T, _]): TypedAggregate[T, Long] =
    sparkFunctions.approx_count_distinct(column.untyped).typedAggregate

  /** Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @param rsd maximum estimation error allowed (default = 0.05)
    *
    * apache/spark
    */
  def approxCountDistinct[T](column: TypedColumn[T, _], rsd: Double): TypedAggregate[T, Long] =
    sparkFunctions.approx_count_distinct(column.untyped, rsd).typedAggregate

  /** Aggregate function: returns a list of objects with duplicates.
    *
    * apache/spark
    */
  def collectList[T, A: TypedEncoder](column: TypedColumn[T, A]): TypedAggregate[T, Vector[A]] =
    sparkFunctions.collect_list(column.untyped).typedAggregate

  /** Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * apache/spark
    */
  def collectSet[T, A: TypedEncoder](column: TypedColumn[T, A]): TypedAggregate[T, Vector[A]] =
    sparkFunctions.collect_set(column.untyped).typedAggregate

  /** Aggregate function: returns the sum of all values in the given column.
    *
    * apache/spark
    */
  def sum[A, T, Out](column: TypedColumn[T, A])(
    implicit
    summable: CatalystSummable[A, Out],
    oencoder: TypedEncoder[Out]
  ): TypedAggregate[T, Out] = {
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].catalystRepr)
    val sumExpr = expr(sparkFunctions.sum(column.untyped))
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
    val zeroExpr = Literal.create(summable.zero, TypedEncoder[Out].catalystRepr)
    val sumExpr = expr(sparkFunctions.sumDistinct(column.untyped))
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
    new TypedAggregate[T, Out](sparkFunctions.avg(column.untyped))
  }

  /** Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @note In Spark variance always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#186]]
    *
    * apache/spark
    */
  def variance[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregate[T, Double] =
    sparkFunctions.variance(column.untyped).typedAggregate

  /** Aggregate function: returns the sample standard deviation.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#155]]
    *
    * apache/spark
    */
  def stddev[A: CatalystVariance, T](column: TypedColumn[T, A]): TypedAggregate[T, Double] =
    sparkFunctions.stddev(column.untyped).typedAggregate

  /**
    * Aggregate function: returns the standard deviation of a column by population.
    *
    * @note In Spark stddev always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala#L143]]
    *
    *       apache/spark
    */
  def stddevPop[A, T](column: TypedColumn[T, A])(implicit ev: CatalystCast[A, Double]): TypedAggregate[T, Option[Double]] = {
    new TypedAggregate[T, Option[Double]](
      sparkFunctions.stddev_pop(column.cast[Double].untyped)
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
  def stddevSamp[A, T](column: TypedColumn[T, A])(implicit ev: CatalystCast[A, Double] ): TypedAggregate[T, Option[Double]] = {
    new TypedAggregate[T, Option[Double]](
      sparkFunctions.stddev_samp(column.cast[Double].untyped)
    )
  }

  /** Aggregate function: returns the maximum value of the column in a group.
    *
    * apache/spark
    */
  def max[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    sparkFunctions.max(column.untyped).typedAggregate
  }

  /** Aggregate function: returns the minimum value of the column in a group.
    *
    * apache/spark
    */
  def min[A: CatalystOrdered, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    implicit val c = column.uencoder
    sparkFunctions.min(column.untyped).typedAggregate
  }

  /** Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * apache/spark
    */
  def first[A, T](column: TypedColumn[T, A]): TypedAggregate[T, A] = {
    sparkFunctions.first(column.untyped).typedAggregate(column.uencoder)
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
    sparkFunctions.last(column.untyped).typedAggregate
  }

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @note In Spark corr always returns Double
    *       [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Corr.scala#L95]]
    *
    *       apache/spark
    */
  def corr[A, B, T](column1: TypedColumn[T, A], column2: TypedColumn[T, B])
    (implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedAggregate[T, Option[Double]] = {
      new TypedAggregate[T, Option[Double]](
        sparkFunctions.corr(column1.cast[Double].untyped, column2.cast[Double].untyped)
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
  def covarPop[A, B, T](column1: TypedColumn[T, A], column2: TypedColumn[T, B])
    (implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedAggregate[T, Option[Double]] = {
      new TypedAggregate[T, Option[Double]](
        sparkFunctions.covar_pop(column1.cast[Double].untyped, column2.cast[Double].untyped)
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
  def covarSamp[A, B, T](column1: TypedColumn[T, A], column2: TypedColumn[T, B])
    (implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedAggregate[T, Option[Double]] = {
      new TypedAggregate[T, Option[Double]](
        sparkFunctions.covar_samp(column1.cast[Double].untyped, column2.cast[Double].untyped)
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
  def kurtosis[A, T](column: TypedColumn[T, A])(implicit ev: CatalystCast[A, Double]): TypedAggregate[T, Option[Double]] = {
    new TypedAggregate[T, Option[Double]](
      sparkFunctions.kurtosis(column.cast[Double].untyped)
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
  def skewness[A, T](column: TypedColumn[T, A])(implicit ev: CatalystCast[A, Double]): TypedAggregate[T, Option[Double]] = {
    new TypedAggregate[T, Option[Double]](
      sparkFunctions.skewness(column.cast[Double].untyped)
    )
  }
}
