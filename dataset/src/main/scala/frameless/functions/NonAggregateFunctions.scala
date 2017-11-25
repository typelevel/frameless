package frameless
package functions

import frameless._
import org.apache.spark.sql.{functions => untyped}

trait NonAggregateFunctions {
  /** Non-Aggregate function: returns the absolute value of a numeric column
    *
    * apache/spark
    */
  def abs[A, B, T](column: TypedColumn[T, A])(implicit evAbs: CatalystAbsolute[A, B], enc:TypedEncoder[B]):TypedColumn[T, B] = {
    implicit val c = column.uencoder
    new TypedColumn[T, B](untyped.abs(column.untyped))
  }

  /** Non-Aggregate function: returns the acos of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def acos[A, T](column: TypedColumn[T, A])
    (implicit evCanBeDouble: CatalystCast[A, Double]): TypedColumn[T, Double] = {
    implicit val c = column.uencoder
    new TypedColumn[T, Double](untyped.acos(column.cast[Double].untyped))
  }



  /** Non-Aggregate function: returns true if value is contained with in the array in the specified column
    *
    * apache/spark
    */
  def arrayContains[C[_]: CatalystCollection, A, T](column: TypedColumn[T, C[A]], value: A): TypedColumn[T, Boolean] = {
    implicit val c = column.uencoder
    new TypedColumn[T, Boolean](untyped.array_contains(column.untyped, value))
  }

  /** Non-Aggregate function: takes the first letter of a string column and returns the ascii int value in a new column
    *
    * apache/spark
    */
  def ascii[T](column: TypedColumn[T, String]): TypedColumn[T, Int] = {
    implicit val c = column.uencoder
    new TypedColumn[T, Int](untyped.ascii(column.untyped))
  }

  /** Non-Aggregate function: returns the atan of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan[A, T](column: TypedColumn[T,A])
                (implicit evCanBeDouble: CatalystCast[A, Double]): TypedColumn[T, Double] = {
    implicit val c = column.uencoder
    new TypedColumn[T, Double](untyped.atan(column.cast[Double].untyped))
  }

  /** Non-Aggregate function: returns the asin of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def asin[A, T](column: TypedColumn[T, A])
                (implicit evCanBeDouble: CatalystCast[A, Double]): TypedColumn[T, Double] = {
    implicit val c = column.uencoder
    new TypedColumn[T, Double](untyped.asin(column.cast[Double].untyped))
  }

  /** Non-Aggregate function: returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan2[A, B, T](l: TypedColumn[T,A], r: TypedColumn[T, B])
    (implicit
      evCanBeDoubleL: CatalystCast[A, Double],
      evCanBeDoubleR: CatalystCast[B, Double]
    ): TypedColumn[T, Double] = {
      implicit val lUnencoder = l.uencoder
      implicit val rUnencoder = r.uencoder
      new TypedColumn[T, Double](untyped.atan2(l.cast[Double].untyped, r.cast[Double].untyped))
    }

  def atan2[B, T](l: Double, r: TypedColumn[T, B])(implicit evCanBeDoubleR: CatalystCast[B, Double]): TypedColumn[T, Double] =
    atan2(lit(l): TypedColumn[T, Double], r)

  def atan2[A, T](l: TypedColumn[T, A], r: Double)(implicit evCanBeDoubleL: CatalystCast[A, Double]): TypedColumn[T, Double] =
    atan2(l, lit(r): TypedColumn[T, Double])

  /** Non-Aggregate function: Computes the BASE64 encoding of a binary column and returns it as a string column.
    * This is the reverse of unbase64.
    *
    * apache/spark
    */
  def base64[T](column: TypedColumn[T, Array[Byte]]): TypedColumn[T, String] = {
    implicit val c = column.uencoder
    new TypedColumn[T, String](untyped.base64(column.untyped))
  }

  /** Non-Aggregate function: Returns the string representation of the binary value of the given long
    * column. For example, bin("12") returns "1100".
    *
    * apache/spark
    */
  def bin[T](column: TypedColumn[T, Long]): TypedColumn[T, String] = {
    implicit val c = column.uencoder
    new TypedColumn[T, String](untyped.bin(column.untyped))
  }

  /** Non-Aggregate function: Computes bitwise NOT.
    *
    * apache/spark
    */
  def bitwiseNOT[A: CatalystBitwise, T](column: TypedColumn[T, A]): TypedColumn[T, A] = {
    implicit val c = column.uencoder
    new TypedColumn[T, A](untyped.bitwiseNOT(column.untyped))
  }
}
