package frameless
package functions

import org.apache.spark.sql.{ Column, functions => sparkFunctions }

import scala.annotation.nowarn
import scala.util.matching.Regex

trait NonAggregateFunctions {

  /**
   * Non-Aggregate function: calculates the SHA-2 digest of a binary column and returns the value as a 40 character hex string
   *
   * apache/spark
   */
  def sha2[T](
      column: AbstractTypedColumn[T, Array[Byte]],
      numBits: Int
    ): column.ThisType[T, String] =
    column.typed(sparkFunctions.sha2(column.untyped, numBits))

  /**
   * Non-Aggregate function: calculates the SHA-1 digest of a binary column and returns the value as a 40 character hex string
   *
   * apache/spark
   */
  def sha1[T](column: AbstractTypedColumn[T, Array[Byte]]): column.ThisType[T, String] =
    column.typed(sparkFunctions.sha1(column.untyped))

  /**
   * Non-Aggregate function: returns a cyclic redundancy check value of a binary column as long.
   *
   * apache/spark
   */
  def crc32[T](column: AbstractTypedColumn[T, Array[Byte]]): column.ThisType[T, Long] =
    column.typed(sparkFunctions.crc32(column.untyped))

  /**
   * Non-Aggregate function: returns the negated value of column.
   *
   * apache/spark
   */
  def negate[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.negate(column.untyped))

  /**
   * Non-Aggregate function: logical not.
   *
   * apache/spark
   */
  def not[T](column: AbstractTypedColumn[T, Boolean]): column.ThisType[T, Boolean] =
    column.typed(sparkFunctions.not(column.untyped))

  /**
   * Non-Aggregate function: Convert a number in a string column from one base to another.
   *
   * apache/spark
   */
  def conv[T](
      column: AbstractTypedColumn[T, String],
      fromBase: Int,
      toBase: Int
    ): column.ThisType[T, String] =
    column.typed(sparkFunctions.conv(column.untyped, fromBase, toBase))

  /**
   * Non-Aggregate function: Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * apache/spark
   */
  def degrees[A, T](column: AbstractTypedColumn[T, A]): column.ThisType[T, Double] =
    column.typed(sparkFunctions.degrees(column.untyped))

  /**
   * Non-Aggregate function: returns the ceiling of a numeric column
   *
   * apache/spark
   */
  def ceil[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystRound[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.ceil(column.untyped))(i1)

  /**
   * Non-Aggregate function: returns the floor of a numeric column
   *
   * apache/spark
   */
  def floor[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystRound[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.floor(column.untyped))(i1)

  /**
   * Non-Aggregate function: unsigned shift the the given value numBits right. If given long, will return long else it will return an integer.
   *
   * apache/spark
   */
  @nowarn // supress sparkFunctions.shiftRightUnsigned call which is used to maintain Spark 3.1.x backwards compat
  def shiftRightUnsigned[A, B, T](
      column: AbstractTypedColumn[T, A],
      numBits: Int
    )(implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.shiftRightUnsigned(column.untyped, numBits))

  /**
   * Non-Aggregate function: shift the the given value numBits right. If given long, will return long else it will return an integer.
   *
   * apache/spark
   */
  @nowarn // supress sparkFunctions.shiftReft call which is used to maintain Spark 3.1.x backwards compat
  def shiftRight[A, B, T](
      column: AbstractTypedColumn[T, A],
      numBits: Int
    )(implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.shiftRight(column.untyped, numBits))

  /**
   * Non-Aggregate function: shift the the given value numBits left. If given long, will return long else it will return an integer.
   *
   * apache/spark
   */
  @nowarn // supress sparkFunctions.shiftLeft call which is used to maintain Spark 3.1.x backwards compat
  def shiftLeft[A, B, T](
      column: AbstractTypedColumn[T, A],
      numBits: Int
    )(implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.shiftLeft(column.untyped, numBits))

  /**
   * Non-Aggregate function: returns the absolute value of a numeric column
   *
   * apache/spark
   */
  def abs[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.abs(column.untyped))(i1)

  /**
   * Non-Aggregate function: Computes the cosine of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def cos[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.cos(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the hyperbolic cosine of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def cosh[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.cosh(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the signum of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def signum[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.signum(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the sine of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def sin[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.sin(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the hyperbolic sine of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def sinh[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.sinh(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the tangent of the given column.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def tan[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.tan(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Computes the hyperbolic tangent of the given value.
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def tanh[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.tanh(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns the acos of a numeric column
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def acos[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.acos(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns true if value is contained with in the array in the specified column
   *
   * apache/spark
   */
  def arrayContains[C[_]: CatalystCollection, A, T](
      column: AbstractTypedColumn[T, C[A]],
      value: A
    ): column.ThisType[T, Boolean] =
    column.typed(sparkFunctions.array_contains(column.untyped, value))

  /**
   * Non-Aggregate function: returns the atan of a numeric column
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def atan[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.atan(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns the asin of a numeric column
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def asin[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.asin(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def atan2[A, B, T](
      l: TypedColumn[T, A],
      r: TypedColumn[T, B]
    )(implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedColumn[T, Double] =
    r.typed(
      sparkFunctions.atan2(l.cast[Double].untyped, r.cast[Double].untyped)
    )

  /**
   * Non-Aggregate function: returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * Spark will expect a Double value for this expression. See:
   *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
   * apache/spark
   */
  def atan2[A, B, T](
      l: TypedAggregate[T, A],
      r: TypedAggregate[T, B]
    )(implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedAggregate[T, Double] =
    r.typed(
      sparkFunctions.atan2(l.cast[Double].untyped, r.cast[Double].untyped)
    )

  def atan2[B, T](
      l: Double,
      r: TypedColumn[T, B]
    )(implicit
      i0: CatalystCast[B, Double]
    ): TypedColumn[T, Double] =
    atan2(r.lit(l), r)

  def atan2[A, T](
      l: TypedColumn[T, A],
      r: Double
    )(implicit
      i0: CatalystCast[A, Double]
    ): TypedColumn[T, Double] =
    atan2(l, l.lit(r))

  def atan2[B, T](
      l: Double,
      r: TypedAggregate[T, B]
    )(implicit
      i0: CatalystCast[B, Double]
    ): TypedAggregate[T, Double] =
    atan2(r.lit(l), r)

  def atan2[A, T](
      l: TypedAggregate[T, A],
      r: Double
    )(implicit
      i0: CatalystCast[A, Double]
    ): TypedAggregate[T, Double] =
    atan2(l, l.lit(r))

  /**
   * Non-Aggregate function: returns the square root value of a numeric column.
   *
   * apache/spark
   */
  def sqrt[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.sqrt(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns the cubic root value of a numeric column.
   *
   * apache/spark
   */
  def cbrt[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.cbrt(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: returns the exponential value of a numeric column.
   *
   * apache/spark
   */
  def exp[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.exp(column.cast[Double].untyped))

  /**
   * Non-Aggregate function: Returns the value of the column `e` rounded to 0 decimal places with HALF_UP round mode.
   *
   * apache/spark
   */
  def round[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.round(column.untyped))(i1)

  /**
   * Non-Aggregate function: Round the value of `e` to `scale` decimal places with HALF_UP round mode
   * if `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * apache/spark
   */
  def round[A, B, T](
      column: AbstractTypedColumn[T, A],
      scale: Int
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.round(column.untyped, scale))(i1)

  /**
   * Non-Aggregate function: Bankers Rounding - returns the rounded to 0 decimal places value with HALF_EVEN round mode
   *  of a numeric column.
   *
   * apache/spark
   */
  def bround[A, B, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.bround(column.untyped))(i1)

  /**
   * Non-Aggregate function: Bankers Rounding - returns the rounded to `scale` decimal places value with HALF_EVEN round mode
   *  of a numeric column. If `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * apache/spark
   */
  def bround[A, B, T](
      column: AbstractTypedColumn[T, A],
      scale: Int
    )(implicit
      i0: CatalystNumericWithJavaBigDecimal[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
    column.typed(sparkFunctions.bround(column.untyped, scale))(i1)

  /**
   * Computes the natural logarithm of the given value.
   *
   * apache/spark
   */
  def log[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.log(column.untyped))

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * apache/spark
   */
  def log[A, T](
      base: Double,
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.log(base, column.untyped))

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * apache/spark
   */
  def log2[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.log2(column.untyped))

  /**
   * Computes the natural logarithm of the given value plus one.
   *
   * apache/spark
   */
  def log1p[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.log1p(column.untyped))

  /**
   * Computes the logarithm of the given column in base 10.
   *
   * apache/spark
   */
  def log10[A, T](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.log10(column.untyped))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * apache/spark
   */
  def hypot[A, T](
      column: AbstractTypedColumn[T, A],
      column2: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.hypot(column.untyped, column2.untyped))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * apache/spark
   */
  def hypot[A, T](
      column: AbstractTypedColumn[T, A],
      l: Double
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.hypot(column.untyped, l))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * apache/spark
   */
  def hypot[A, T](
      l: Double,
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.hypot(l, column.untyped))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * apache/spark
   */
  def pow[A, T](
      column: AbstractTypedColumn[T, A],
      column2: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.pow(column.untyped, column2.untyped))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * apache/spark
   */
  def pow[A, T](
      column: AbstractTypedColumn[T, A],
      l: Double
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.pow(column.untyped, l))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * apache/spark
   */
  def pow[A, T](
      l: Double,
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: CatalystCast[A, Double]
    ): column.ThisType[T, Double] =
    column.typed(sparkFunctions.pow(l, column.untyped))

  /**
   * Returns the positive value of dividend mod divisor.
   *
   * apache/spark
   */
  def pmod[A, T](
      column: AbstractTypedColumn[T, A],
      column2: AbstractTypedColumn[T, A]
    )(implicit
      i0: TypedEncoder[A]
    ): column.ThisType[T, A] =
    column.typed(sparkFunctions.pmod(column.untyped, column2.untyped))

  /**
   * Non-Aggregate function: Returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * apache/spark
   */
  def bin[T](column: AbstractTypedColumn[T, Long]): column.ThisType[T, String] =
    column.typed(sparkFunctions.bin(column.untyped))

  /**
   * Calculates the MD5 digest of a binary column and returns the value
   * as a 32 character hex string.
   *
   * apache/spark
   */
  def md5[T, A](
      column: AbstractTypedColumn[T, A]
    )(implicit
      i0: TypedEncoder[A]
    ): column.ThisType[T, String] =
    column.typed(sparkFunctions.md5(column.untyped))

  /**
   * Computes the factorial of the given value.
   *
   * apache/spark
   */
  def factorial[T](
      column: AbstractTypedColumn[T, Long]
    )(implicit
      i0: TypedEncoder[Long]
    ): column.ThisType[T, Long] =
    column.typed(sparkFunctions.factorial(column.untyped))

  /**
   * Non-Aggregate function: Computes bitwise NOT.
   *
   * apache/spark
   */
  @nowarn // supress sparkFunctions.bitwiseNOT call which is used to maintain Spark 3.1.x backwards compat
  def bitwiseNOT[A: CatalystBitwise, T](
      column: AbstractTypedColumn[T, A]
    ): column.ThisType[T, A] =
    column.typed(sparkFunctions.bitwiseNOT(column.untyped))(column.uencoder)

  /**
   * Non-Aggregate function: file name of the current Spark task. Empty string if row did not originate from
   * a file
   *
   * apache/spark
   */
  def inputFileName[T](): TypedColumn[T, String] =
    new TypedColumn[T, String](sparkFunctions.input_file_name())

  /**
   * Non-Aggregate function: generates monotonically increasing id
   *
   * apache/spark
   */
  def monotonicallyIncreasingId[T](): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](sparkFunctions.monotonically_increasing_id())
  }

  /**
   * Non-Aggregate function: Evaluates a list of conditions and returns one of multiple
   * possible result expressions. If none match, otherwise is returned
   * {{{
   *   when(ds('boolField), ds('a))
   *     .when(ds('otherBoolField), lit(123))
   *     .otherwise(ds('b))
   * }}}
   * apache/spark
   */
  def when[T, A](
      condition: AbstractTypedColumn[T, Boolean],
      value: AbstractTypedColumn[T, A]
    ): When[T, A] =
    new When[T, A](condition, value)

  class When[T, A] private (untypedC: Column) {
    private[functions] def this(
        condition: AbstractTypedColumn[T, Boolean],
        value: AbstractTypedColumn[T, A]
      ) =
      this(sparkFunctions.when(condition.untyped, value.untyped))

    def when(
        condition: AbstractTypedColumn[T, Boolean],
        value: AbstractTypedColumn[T, A]
      ): When[T, A] =
      new When[T, A](untypedC.when(condition.untyped, value.untyped))

    def otherwise(value: AbstractTypedColumn[T, A]): value.ThisType[T, A] =
      value.typed(untypedC.otherwise(value.untyped))(value.uencoder)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Non-Aggregate function: takes the first letter of a string column and returns the ascii int value in a new column
   *
   * apache/spark
   */
  def ascii[T](column: AbstractTypedColumn[T, String]): column.ThisType[T, Int] =
    column.typed(sparkFunctions.ascii(column.untyped))

  /**
   * Non-Aggregate function: Computes the BASE64 encoding of a binary column and returns it as a string column.
   * This is the reverse of unbase64.
   *
   * apache/spark
   */
  def base64[T](column: AbstractTypedColumn[T, Array[Byte]]): column.ThisType[T, String] =
    column.typed(sparkFunctions.base64(column.untyped))

  /**
   * Non-Aggregate function: Decodes a BASE64 encoded string column and returns it as a binary column.
   * This is the reverse of base64.
   *
   * apache/spark
   */
  def unbase64[T](column: AbstractTypedColumn[T, String]): column.ThisType[T, Array[Byte]] =
    column.typed(sparkFunctions.unbase64(column.untyped))

  /**
   * Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
   * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
   *
   * apache/spark
   */
  def concat[T](columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(sparkFunctions.concat(columns.map(_.untyped): _*))

  /**
   * Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
   * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
   *
   * apache/spark
   */
  def concat[T](columns: TypedAggregate[T, String]*): TypedAggregate[T, String] =
    new TypedAggregate(sparkFunctions.concat(columns.map(_.untyped): _*))

  /**
   * Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
   * using the given separator.
   * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
   *
   * apache/spark
   */
  def concatWs[T](
      sep: String,
      columns: TypedAggregate[T, String]*
    ): TypedAggregate[T, String] =
    new TypedAggregate(
      sparkFunctions.concat_ws(sep, columns.map(_.untyped): _*)
    )

  /**
   * Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
   * using the given separator.
   * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
   *
   * apache/spark
   */
  def concatWs[T](sep: String, columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(sparkFunctions.concat_ws(sep, columns.map(_.untyped): _*))

  /**
   * Non-Aggregate function: Locates the position of the first occurrence of substring column
   * in given string
   *
   * @note The position is not zero based, but 1 based index. Returns 0 if substr
   * could not be found in str.
   *
   * apache/spark
   */
  def instr[T](
      str: AbstractTypedColumn[T, String],
      substring: String
    ): str.ThisType[T, Int] =
    str.typed(sparkFunctions.instr(str.untyped, substring))

  /**
   * Non-Aggregate function: Computes the length of a given string.
   *
   * apache/spark
   */
  // TODO: Also for binary
  def length[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Int] =
    str.typed(sparkFunctions.length(str.untyped))

  /**
   * Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
   *
   * apache/spark
   */
  def levenshtein[T](
      l: TypedColumn[T, String],
      r: TypedColumn[T, String]
    ): TypedColumn[T, Int] =
    l.typed(sparkFunctions.levenshtein(l.untyped, r.untyped))

  /**
   * Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
   *
   * apache/spark
   */
  def levenshtein[T](
      l: TypedAggregate[T, String],
      r: TypedAggregate[T, String]
    ): TypedAggregate[T, Int] =
    l.typed(sparkFunctions.levenshtein(l.untyped, r.untyped))

  /**
   * Non-Aggregate function: Converts a string column to lower case.
   *
   * apache/spark
   */
  def lower[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.lower(str.untyped))

  /**
   * Non-Aggregate function: Left-pad the string column with pad to a length of len. If the string column is longer
   * than len, the return value is shortened to len characters.
   *
   * apache/spark
   */
  def lpad[T](
      str: AbstractTypedColumn[T, String],
      len: Int,
      pad: String
    ): str.ThisType[T, String] =
    str.typed(sparkFunctions.lpad(str.untyped, len, pad))

  /**
   * Non-Aggregate function: Trim the spaces from left end for the specified string value.
   *
   * apache/spark
   */
  def ltrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.ltrim(str.untyped))

  /**
   * Non-Aggregate function: Replace all substrings of the specified string value that match regexp with rep.
   *
   * apache/spark
   */
  def regexpReplace[T](
      str: AbstractTypedColumn[T, String],
      pattern: Regex,
      replacement: String
    ): str.ThisType[T, String] =
    str.typed(
      sparkFunctions.regexp_replace(str.untyped, pattern.regex, replacement)
    )

  /**
   * Non-Aggregate function: Reverses the string column and returns it as a new string column.
   *
   * apache/spark
   */
  def reverse[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.reverse(str.untyped))

  /**
   * Non-Aggregate function: Right-pad the string column with pad to a length of len.
   * If the string column is longer than len, the return value is shortened to len characters.
   *
   * apache/spark
   */
  def rpad[T](
      str: AbstractTypedColumn[T, String],
      len: Int,
      pad: String
    ): str.ThisType[T, String] =
    str.typed(sparkFunctions.rpad(str.untyped, len, pad))

  /**
   * Non-Aggregate function: Trim the spaces from right end for the specified string value.
   *
   * apache/spark
   */
  def rtrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.rtrim(str.untyped))

  /**
   * Non-Aggregate function: Substring starts at `pos` and is of length `len`
   *
   * apache/spark
   */
  // TODO: Also for byte array
  def substring[T](
      str: AbstractTypedColumn[T, String],
      pos: Int,
      len: Int
    ): str.ThisType[T, String] =
    str.typed(sparkFunctions.substring(str.untyped, pos, len))

  /**
   * Non-Aggregate function: Trim the spaces from both ends for the specified string column.
   *
   * apache/spark
   */
  def trim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.trim(str.untyped))

  /**
   * Non-Aggregate function: Converts a string column to upper case.
   *
   * apache/spark
   */
  def upper[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.upper(str.untyped))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // DateTime functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Non-Aggregate function: Extracts the year as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#year` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def year[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.year(str.untyped))

  /**
   * Non-Aggregate function: Extracts the quarter as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#quarter` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def quarter[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.quarter(str.untyped))

  /**
   * Non-Aggregate function Extracts the month as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#month` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def month[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.month(str.untyped))

  /**
   * Non-Aggregate function: Extracts the day of the week as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#dayofweek` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def dayofweek[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.dayofweek(str.untyped))

  /**
   * Non-Aggregate function: Extracts the day of the month as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#dayofmonth` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def dayofmonth[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.dayofmonth(str.untyped))

  /**
   * Non-Aggregate function: Extracts the day of the year as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#dayofyear` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def dayofyear[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.dayofyear(str.untyped))

  /**
   * Non-Aggregate function: Extracts the hours as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#hour` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def hour[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.hour(str.untyped))

  /**
   * Non-Aggregate function: Extracts the minutes as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#minute` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def minute[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.minute(str.untyped))

  /**
   * Non-Aggregate function: Extracts the seconds as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#second` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def second[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.second(str.untyped))

  /**
   * Non-Aggregate function: Extracts the week number as an integer from a given date/timestamp/string.
   *
   * Differs from `Column#weekofyear` by wrapping it's result into an `Option`.
   *
   * apache/spark
   */
  def weekofyear[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(sparkFunctions.weekofyear(str.untyped))
}
