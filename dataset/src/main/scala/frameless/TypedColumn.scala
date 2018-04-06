package frameless

import frameless.functions.{lit => flit, litAggr}
import frameless.syntax._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, FramelessInternals}
import shapeless._
import shapeless.ops.record.Selector

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

sealed trait UntypedExpression[T] {
  def expr: Expression
  def uencoder: TypedEncoder[_]
  override def toString: String = expr.toString()
}

/** Expression used in `select`-like constructions.
  */
sealed class TypedColumn[T, U](expr: Expression)(
  implicit val uenc: TypedEncoder[U]
) extends AbstractTypedColumn[T, U](expr) {

  type ThisType[A, B] = TypedColumn[A, B]

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  override def typed[W, U1: TypedEncoder](c: Column): TypedColumn[W, U1] = c.typedColumn
  override def lit[U1: TypedEncoder](c: U1): TypedColumn[T,U1] = flit(c)
}

/** Expression used in `agg`-like constructions.
  */
sealed class TypedAggregate[T, U](expr: Expression)(
  implicit val uenc: TypedEncoder[U]
) extends AbstractTypedColumn[T, U](expr) {

  type ThisType[A, B] = TypedAggregate[A, B]

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  override def typed[W, U1: TypedEncoder](c: Column): TypedAggregate[W, U1] = c.typedAggregate
  override def lit[U1: TypedEncoder](c: U1): TypedAggregate[T, U1] = litAggr(c)
}

/** Generic representation of a typed column. A typed column can either be a [[TypedAggregate]] or
  * a [[frameless.TypedColumn]].
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * @tparam T phantom type representing the dataset on which this columns is
  *           selected. When `T = A with B` the selection is on either A or B.
  * @tparam U type of column
  */
abstract class AbstractTypedColumn[T, U]
  (val expr: Expression)
  (implicit val uencoder: TypedEncoder[U])
    extends UntypedExpression[T] { self =>

  type ThisType[A, B] <: AbstractTypedColumn[A, B]

  /** Fall back to an untyped Column */
  def untyped: Column = new Column(expr)

  private def equalsTo[TT, W](other: ThisType[TT, U])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] = typed {
    if (uencoder.nullable && uencoder.catalystRepr.typeName != "struct") EqualNullSafe(self.expr, other.expr)
    else EqualTo(self.expr, other.expr)
  }

  /** Creates a typed column of either TypedColumn or TypedAggregate from an expression. */
  protected def typed[W, U1: TypedEncoder](e: Expression): ThisType[W, U1] =
    typed(new Column(e))

  /** Creates a typed column of either TypedColumn or TypedAggregate. */
  def typed[W, U1: TypedEncoder](c: Column): ThisType[W, U1]

  /** Creates a typed column of either TypedColumn or TypedAggregate. */
  def lit[U1: TypedEncoder](c: U1): ThisType[T, U1]

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(u: U): ThisType[T, Boolean] =
    equalsTo(lit(u))

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===[TT, W](other: ThisType[TT, U])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    equalsTo(other)

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def =!=[TT, W](other: ThisType[TT, U])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(Not(equalsTo(other).expr))

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= "a" )
    * }}}
    *
    * apache/spark
    */
  def =!=(u: U): ThisType[T, Boolean] =
    typed(Not(equalsTo(lit(u)).expr))

  /** True if the current expression is an Option and it's None.
    *
    * apache/spark
    */
  def isNone(implicit i0: U <:< Option[_]): ThisType[T, Boolean] =
    equalsTo[T, T](lit[U](None.asInstanceOf[U]))

  /** True if the current expression is an Option and it's not None.
    *
    * apache/spark
    */
  def isNotNone(implicit i0: U <:< Option[_]): ThisType[T, Boolean] =
    typed(Not(equalsTo(lit(None.asInstanceOf[U])).expr))

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(df('defaultValue)) )
    * }}}
    */
  def getOrElse[TT, W, Out](default: ThisType[TT, Out])(implicit i0: U =:= Option[Out], i1: With.Aux[T, TT, W]): ThisType[W, Out] =
    typed(Coalesce(Seq(expr, default.expr)))(default.uencoder)

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(defaultConstant) )
    * }}}
    */
  def getOrElse[Out: TypedEncoder](default: Out)(implicit i0: U =:= Option[Out]): ThisType[T, Out] =
    getOrElse(lit[Out](default))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus[TT, W](other: ThisType[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    typed(self.untyped.plus(other.untyped))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +[TT, W](other: ThisType[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    plus(other)

  /** Sum of this expression (column) with a constant.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people('height) + 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def +(u: U)(implicit n: CatalystNumeric[U]): ThisType[T, U] =
    typed(self.untyped.plus(u))

  /** Unary minus, i.e. negate the expression.
    * {{{
    *   // Select the amount column and negates all values.
    *   df.select( -df('amount) )
    * }}}
    *
    * apache/spark
    */
  def unary_-(implicit n: CatalystNumeric[U]): ThisType[T, U] =
    typed(-self.untyped)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus[TT, W](other: ThisType[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    typed(self.untyped.minus(other.untyped))

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -[TT, W](other: ThisType[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    minus(other)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people('height) - 1 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def -(u: U)(implicit n: CatalystNumeric[U]): ThisType[T, U] =
    typed(self.untyped.minus(u))

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply[TT, W]
    (other: ThisType[TT, U])
    (implicit
      n: CatalystNumeric[U],
      w: With.Aux[T, TT, W],
      t: ClassTag[U]
    ): ThisType[W, U] = typed {
      if (t.runtimeClass == BigDecimal(0).getClass) {
        // That's apparently the only way to get sound multiplication.
        // See https://issues.apache.org/jira/browse/SPARK-22036
        val dt = DecimalType(20, 14)
        self.untyped.cast(dt).multiply(other.untyped.cast(dt))
      } else {
        self.untyped.multiply(other.untyped)
      }
    }

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *[TT, W](other: ThisType[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W], t: ClassTag[U]): ThisType[W, U] =
    multiply(other)

  /** Multiplication of this expression a constant.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: U)(implicit n: CatalystNumeric[U]): ThisType[T, U] =
    typed(self.untyped.multiply(u))

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def divide[Out: TypedEncoder, TT, W](other: ThisType[TT, U])(implicit n: CatalystDivisible[U, Out], w: With.Aux[T, TT, W]): ThisType[W, Out] =
    typed(self.untyped.divide(other.untyped))

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def /[Out, TT, W](other: ThisType[TT, U])(implicit n: CatalystDivisible[U, Out], e: TypedEncoder[Out], w: With.Aux[T, TT, W]): ThisType[W, Out] =
    divide(other)

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def /(u: U)(implicit n: CatalystNumeric[U]): ThisType[T, Double] =
    typed(self.untyped.divide(u))

  /** Returns a descending ordering used in sorting
    *
    * apache/spark
    */
  def desc(implicit catalystOrdered: CatalystOrdered[U]): SortedTypedColumn[T, U] =
    new SortedTypedColumn[T, U](untyped.desc)

  /** Returns an ascending ordering used in sorting
    *
    * apache/spark
    */
  def asc(implicit catalystOrdered: CatalystOrdered[U]): SortedTypedColumn[T, U] =
    new SortedTypedColumn[T, U](untyped.asc)

  /** Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    typed(self.untyped.bitwiseAND(u))

  /** Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    typed(self.untyped.bitwiseAND(other.untyped))

  /** Bitwise AND this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Int] & -1)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def &(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    bitwiseAND(u)

  /** Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) & (df.col('colB)))
    * }}}
    *
    * @param other a constant of the same type
    * apache/spark
    */
  def &[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    bitwiseAND(other)

  /** Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseOR(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    typed(self.untyped.bitwiseOR(u))

  /** Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param other a constant of the same type
    * apache/spark
    */
  def bitwiseOR[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    typed(self.untyped.bitwiseOR(other.untyped))

  /** Bitwise OR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] | 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def |(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    bitwiseOR(u)

  /** Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) | (df.col('colB)))
    * }}}
    *
    * @param other a constant of the same type
    * apache/spark
    */
  def |[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    bitwiseOR(other)

  /** Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseXOR(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    typed(self.untyped.bitwiseXOR(u))

  /** Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param other a constant of the same type
    * apache/spark
    */
  def bitwiseXOR[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    typed(self.untyped.bitwiseXOR(other.untyped))

  /** Bitwise XOR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] ^ 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def ^(u: U)(implicit n: CatalystBitwise[U]): ThisType[T, U] =
    bitwiseXOR(u)

  /** Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) ^ (df.col('colB)))
    * }}}
    *
    * @param other a constant of the same type
    * apache/spark
    */
  def ^[TT, W](other: ThisType[TT, U])(implicit n: CatalystBitwise[U], w: With.Aux[T, TT, W]): ThisType[W, U] =
    bitwiseXOR(other)

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): ThisType[T, A] =
    typed(self.untyped.cast(TypedEncoder[A].catalystRepr))

  /**
    * An expression that returns a substring
    * {{{
    *   df.select(df('a).substr(0, 5))
    * }}}
    *
    * @param startPos starting position
    * @param len length of the substring
    */
  def substr(startPos: Int, len: Int)(implicit ev: U =:= String): ThisType[T, String] =
    typed(self.untyped.substr(startPos, len))

  /**
    * An expression that returns a substring
    * {{{
    *   df.select(df('a).substr(df('b), df('c)))
    * }}}
    *
    * @param startPos expression for the starting position
    * @param len expression for the length of the substring
    */
  def substr[TT1, TT2, W1, W2](startPos: ThisType[TT1, Int], len: ThisType[TT2, Int])
                   (implicit
                    ev: U =:= String,
                    w1: With.Aux[T, TT1, W1],
                    w2: With.Aux[W1, TT2, W2]): ThisType[W2, String] =
    typed(self.untyped.substr(startPos.untyped, len.untyped))

  /** String contains another string literal.
    * {{{
    *   df.filter ( df.col('a).contains("foo") )
    * }}}
    *
    * @param other a string that is being tested against.
    * apache/spark
    */
  def contains(other: String)(implicit ev: U =:= String): ThisType[T, Boolean] =
    typed(self.untyped.contains(other))

  /** String contains.
    * {{{
    *   df.filter ( df.col('a).contains(df.col('b) )
    * }}}
    *
    * @param other a column which values is used as a string that is being tested against.
    * apache/spark
    */
  def contains[TT, W](other: ThisType[TT, U])(implicit ev: U =:= String, w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped.contains(other.untyped))

  /** String starts with another string literal.
    * {{{
    *   df.filter ( df.col('a).startsWith("foo")
    * }}}
    *
    * @param other a prefix that is being tested against.
    * apache/spark
    */
  def startsWith(other: String)(implicit ev: U =:= String): ThisType[T, Boolean] =
    typed(self.untyped.startsWith(other))

  /** String starts with.
    * {{{
    *   df.filter ( df.col('a).startsWith(df.col('b))
    * }}}
    *
    * @param other a column which values is used as a prefix that is being tested against.
    * apache/spark
    */
  def startsWith[TT, W](other: ThisType[TT, U])(implicit ev: U =:= String, w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped.startsWith(other.untyped))

  /** String ends with another string literal.
    * {{{
    *   df.filter ( df.col('a).endsWith("foo")
    * }}}
    *
    * @param other a suffix that is being tested against.
    * apache/spark
    */
  def endsWith(other: String)(implicit ev: U =:= String): ThisType[T, Boolean] =
    typed(self.untyped.endsWith(other))

  /** String ends with.
    * {{{
    *   df.filter ( df.col('a).endsWith(df.col('b))
    * }}}
    *
    * @param other a column which values is used as a suffix that is being tested against.
    * apache/spark
    */
  def endsWith[TT, W](other: ThisType[TT, U])(implicit ev: U =:= String, w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped.endsWith(other.untyped))

  /** Boolean AND.
    * {{{
    *   df.filter ( (df.col('a) === 1).and(df.col('b) > 5) )
    * }}}
    */
  def and[TT, W](other: ThisType[TT, Boolean])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped.and(other.untyped))

  /** Boolean AND.
    * {{{
    *   df.filter ( df.col('a) === 1 && df.col('b) > 5)
    * }}}
    */
  def && [TT, W](other: ThisType[TT, Boolean])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    and(other)

  /** Boolean OR.
    * {{{
    *   df.filter ( (df.col('a) === 1).or(df.col('b) > 5) )
    * }}}
    */
  def or[TT, W](other: ThisType[TT, Boolean])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped.or(other.untyped))

  /** Boolean OR.
    * {{{
    *   df.filter ( df.col('a) === 1 || df.col('b) > 5)
    * }}}
    */
  def || [TT, W](other: ThisType[TT, Boolean])(implicit w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    or(other)

  /** Less than.
    * {{{
    *   // The following selects people younger than the maxAge column.
    *   df.select( df('age) < df('maxAge) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def <[TT, W](other: ThisType[TT, U])(implicit i0: CatalystOrdered[U], w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped < other.untyped)

  /** Less than or equal to.
    * {{{
    *   // The following selects people younger or equal than the maxAge column.
    *   df.select( df('age) <= df('maxAge)
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def <=[TT, W](other: ThisType[TT, U])(implicit i0: CatalystOrdered[U], w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped <= other.untyped)

  /** Greater than.
    * {{{
    *   // The following selects people older than the maxAge column.
    *   df.select( df('age) > df('maxAge) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def >[TT, W](other: ThisType[TT, U])(implicit i0: CatalystOrdered[U], w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped > other.untyped)

  /** Greater than or equal.
    * {{{
    *   // The following selects people older or equal than the maxAge column.
    *   df.select( df('age) >= df('maxAge) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def >=[TT, W](other: ThisType[TT, U])(implicit i0: CatalystOrdered[U], w: With.Aux[T, TT, W]): ThisType[W, Boolean] =
    typed(self.untyped >= other.untyped)

  /** Less than.
    * {{{
    *   // The following selects people younger than 21.
    *   df.select( df('age) < 21 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def <(u: U)(implicit i0: CatalystOrdered[U]): ThisType[T, Boolean] =
    typed(self.untyped < lit(u)(self.uencoder).untyped)

  /** Less than or equal to.
    * {{{
    *   // The following selects people younger than 22.
    *   df.select( df('age) <= 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def <=(u: U)(implicit i0: CatalystOrdered[U]): ThisType[T, Boolean] =
    typed(self.untyped <= lit(u)(self.uencoder).untyped)

  /** Greater than.
    * {{{
    *   // The following selects people older than 21.
    *   df.select( df('age) > 21 )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >(u: U)(implicit i0: CatalystOrdered[U]): ThisType[T, Boolean] =
    typed(self.untyped > lit(u)(self.uencoder).untyped)

  /** Greater than or equal.
    * {{{
    *   // The following selects people older than 20.
    *   df.select( df('age) >= 21 )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >=(u: U)(implicit i0: CatalystOrdered[U]): ThisType[T, Boolean] =
    typed(self.untyped >= lit(u)(self.uencoder).untyped)

  /**
    * Returns true if the value of this column is contained in of the arguments.
    * {{{
    *   // The following selects people with age 15, 20, or 30.
    *   df.select( df('age).isin(15, 20, 30) )
    * }}}
    *
    * @param values are constants of the same type
    * apache/spark
    */
  def isin(values: U*)(implicit e: CatalystIsin[U]): ThisType[T, Boolean] =
    typed(self.untyped.isin(values:_*))

  /**
    * True if the current column is between the lower bound and upper bound, inclusive.
    *
    * @param lowerBound a constant of the same type
    * @param upperBound a constant of the same type
    * apache/spark
    */
  def between(lowerBound: U, upperBound: U)(implicit i0: CatalystOrdered[U]): ThisType[T, Boolean] =
    typed(self.untyped.between(lit(lowerBound)(self.uencoder).untyped, lit(upperBound)(self.uencoder).untyped))

  /**
    * True if the current column is between the lower bound and upper bound, inclusive.
    *
    * @param lowerBound another column of the same type
    * @param upperBound another column of the same type
    * apache/spark
    */
  def between[TT1, TT2, W1, W2](lowerBound: ThisType[TT1, U], upperBound: ThisType[TT2, U])
    (implicit
      i0: CatalystOrdered[U],
      w0: With.Aux[T, TT1, W1],
      w1: With.Aux[TT2, W1, W2]
    ): ThisType[W2, Boolean] =
      typed(self.untyped.between(lowerBound.untyped, upperBound.untyped))

  /**
    * True if the current column is null.
    * apache/spark
    */
  def isNull(implicit i0:CatalystNullable[U]):ThisType[T,Boolean] =
    typed(self.untyped.isNull)

  /**
    * True if the current column is not null.
    * apache/spark
    */
  def isNotNull(implicit i0:CatalystNullable[U]):ThisType[T,Boolean] =
    typed(self.untyped.isNotNull)
}


sealed class SortedTypedColumn[T, U](val expr: Expression)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] {

  def this(column: Column)(implicit e: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  def untyped: Column = new Column(expr)
}

object SortedTypedColumn {
  implicit def defaultAscending[T, U : CatalystOrdered](typedColumn: TypedColumn[T, U]): SortedTypedColumn[T, U] =
    new SortedTypedColumn[T, U](typedColumn.untyped.asc)(typedColumn.uencoder)

    object defaultAscendingPoly extends Poly1 {
      implicit def caseTypedColumn[T, U : CatalystOrdered] = at[TypedColumn[T, U]](c => defaultAscending(c))
      implicit def caseTypeSortedColumn[T, U] = at[SortedTypedColumn[T, U]](identity)
    }
  }


object TypedColumn {
  /** Evidence that type `T` has column `K` with type `V`. */
  @implicitNotFound(msg = "No column ${K} of type ${V} in ${T}")
  trait Exists[T, K, V]

  @implicitNotFound(msg = "No columns ${K} of type ${V} in ${T}")
  trait ExistsMany[T, K <: HList, V]

  object ExistsMany {
    implicit def deriveCons[T, KH, KT <: HList, V0, V1]
      (implicit
        head: Exists[T, KH, V0],
        tail: ExistsMany[V0, KT, V1]
      ): ExistsMany[T, KH :: KT, V1] =
        new ExistsMany[T, KH :: KT, V1] {}

    implicit def deriveHNil[T, K, V](implicit head: Exists[T, K, V]): ExistsMany[T, K :: HNil, V] =
      new ExistsMany[T, K :: HNil, V] {}
  }

  object Exists {
    def apply[T, V](column: Witness)(implicit e: Exists[T, column.T, V]): Exists[T, column.T, V] = e

    implicit def deriveRecord[T, H <: HList, K, V]
      (implicit
        i0: LabelledGeneric.Aux[T, H],
        i1: Selector.Aux[H, K, V]
      ): Exists[T, K, V] = new Exists[T, K, V] {}
  }
}

/** Compute the intersection of two types:
  *
  * - With[A, A] = A
  * - With[A, B] = A with B (when A != B)
  *
  * This type function is needed to prevent IDEs from infering large types
  * with shape `A with A with ... with A`. These types could be confusing for
  * both end users and IDE's type checkers.
  */
trait With[A, B] { type Out }

trait LowPrioWith {
  type Aux[A, B, W] = With[A, B] { type Out = W }
  protected[this] val theInstance = new With[Any, Any] {}
  protected[this] def of[A, B, W]: With[A, B] { type Out = W } = theInstance.asInstanceOf[Aux[A, B, W]]
  implicit def identity[T]: Aux[T, T, T] = of[T, T, T]
}

object With extends LowPrioWith {
  implicit def combine[A, B]: Aux[A, B, A with B] = of[A, B, A with B]
}
