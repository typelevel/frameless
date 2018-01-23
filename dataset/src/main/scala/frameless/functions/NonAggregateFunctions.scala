package frameless
package functions

import org.apache.spark.sql.{Column, functions => untyped}

import scala.util.matching.Regex

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

  /** Non-Aggregate function: file name of the current Spark task. Empty string if row did not originate from
    * a file
    *
    * apache/spark
    */
  def inputFileName[T](): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.input_file_name())
  }

  /** Non-Aggregate function: generates monotonically increasing id
    *
    * apache/spark
    */
  def monotonicallyIncreasingId[T](): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](untyped.monotonically_increasing_id())
  }

  /** Non-Aggregate function: Evaluates a list of conditions and returns one of multiple
    * possible result expressions. If none match, otherwise is returned
    * {{{
    *   when(ds('boolField), ds('a))
    *     .when(ds('otherBoolField), lit(123))
    *     .otherwise(ds('b))
    * }}}
    * apache/spark
    */
  def when[T, A](condition: TypedColumn[T, Boolean], value: TypedColumn[T, A]): When[T, A] =
    new When[T, A](condition, value)

  class When[T, A] private (untypedC: Column) {
    private[functions] def this(condition: TypedColumn[T, Boolean], value: TypedColumn[T, A]) =
      this(untyped.when(condition.untyped, value.untyped))

    def when(condition: TypedColumn[T, Boolean], value: TypedColumn[T, A]): When[T, A] = new When[T, A](
      untypedC.when(condition.untyped, value.untyped)
    )

    def otherwise(value: TypedColumn[T, A]): TypedColumn[T, A] =
      new TypedColumn[T, A](untypedC.otherwise(value.untyped))(value.uencoder)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////


  /** Non-Aggregate function: takes the first letter of a string column and returns the ascii int value in a new column
    *
    * apache/spark
    */
  def ascii[T](column: TypedColumn[T, String]): TypedColumn[T, Int] = {
    new TypedColumn[T, Int](untyped.ascii(column.untyped))
  }

  /** Non-Aggregate function: Computes the BASE64 encoding of a binary column and returns it as a string column.
    * This is the reverse of unbase64.
    *
    * apache/spark
    */
  def base64[T](column: TypedColumn[T, Array[Byte]]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.base64(column.untyped))
  }

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
    *
    * apache/spark
    */
  def concat[T](columns: TypedColumn[T, String]*): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.concat(columns.map(_.untyped):_*))
  }

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    *
    * apache/spark
    */
  def concatWs[T](sep: String, columns: TypedColumn[T, String]*): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.concat_ws(sep, columns.map(_.untyped):_*))
  }

  /** Non-Aggregate function: Locates the position of the first occurrence of substring column
    * in given string
    *
    * @note The position is not zero based, but 1 based index. Returns 0 if substr
    * could not be found in str.
    *
    * apache/spark
    */
  def instr[T](column: TypedColumn[T, String], substring: String): TypedColumn[T, Int] = {
    new TypedColumn[T, Int](untyped.instr(column.untyped, substring))
  }

  /** Non-Aggregate function: Computes the length of a given string.
    *
    * apache/spark
    */
  //TODO: Also for binary
  def length[T](column: TypedColumn[T, String]): TypedColumn[T, Int] = {
    new TypedColumn[T, Int](untyped.length(column.untyped))
  }

  /** Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
    *
    * apache/spark
    */
  def levenshtein[T](l: TypedColumn[T, String], r: TypedColumn[T, String]): TypedColumn[T, Int] = {
    new TypedColumn[T, Int](untyped.levenshtein(l.untyped, r.untyped))
  }

  /** Non-Aggregate function: Converts a string column to lower case.
    *
    * apache/spark
    */
  def lower[T](e: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.lower(e.untyped))
  }

  /** Non-Aggregate function: Left-pad the string column with pad to a length of len. If the string column is longer
    * than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def lpad[T](str: TypedColumn[T, String], len: Int, pad: String): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.lpad(str.untyped, len, pad))
  }

  /** Non-Aggregate function: Trim the spaces from left end for the specified string value.
    *
    * apache/spark
    */
  def ltrim[T](str: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.ltrim(str.untyped))
  }

  /** Non-Aggregate function: Replace all substrings of the specified string value that match regexp with rep.
    *
    * apache/spark
    */
  def regexpReplace[T](str: TypedColumn[T, String], pattern: Regex, replacement: String): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.regexp_replace(str.untyped, pattern.regex, replacement))
  }

  /** Non-Aggregate function: Reverses the string column and returns it as a new string column.
    *
    * apache/spark
    */
  def reverse[T](str: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.reverse(str.untyped))
  }

  /** Non-Aggregate function: Right-pad the string column with pad to a length of len.
    * If the string column is longer than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def rpad[T](str: TypedColumn[T, String], len: Int, pad: String): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.rpad(str.untyped, len, pad))
  }

  /** Non-Aggregate function: Trim the spaces from right end for the specified string value.
    *
    * apache/spark
    */
  def rtrim[T](e: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.rtrim(e.untyped))
  }

  /** Non-Aggregate function: Substring starts at `pos` and is of length `len`
    *
    * apache/spark
    */
  //TODO: Also for byte array
  def substring[T](str: TypedColumn[T, String], pos: Int, len: Int): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.substring(str.untyped, pos, len))
  }

  /** Non-Aggregate function: Trim the spaces from both ends for the specified string column.
    *
    * apache/spark
    */
  def trim[T](str: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.trim(str.untyped))
  }

  /** Non-Aggregate function: Converts a string column to upper case.
    *
    * apache/spark
    */
  def upper[T](str: TypedColumn[T, String]): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.upper(str.untyped))
  }

  /**
    * Non-Aggregate function: Extracts the year as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#year` by wrapping it's result into an `Option`.
    *
    * apache/spark
    */
  def year[T](col: TypedColumn[T, String]): TypedColumn[T, Option[Int]] = {
    new TypedColumn[T, Option[Int]](untyped.year(col.untyped))
  }
}