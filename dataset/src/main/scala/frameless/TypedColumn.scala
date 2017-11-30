package frameless

import frameless.syntax._
import frameless.functions._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, FramelessInternals}
import org.apache.spark.sql.types.DecimalType
import shapeless.ops.record.Selector
import shapeless._

import scala.reflect.ClassTag
import scala.annotation.implicitNotFound

sealed trait UntypedExpression[T] {
  def expr: Expression
  def uencoder: TypedEncoder[_]
  override def toString: String = expr.toString()
}

/** Expression used in `select`-like constructions.
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * @tparam T type of dataset
  * @tparam U type of column
  */
sealed class TypedColumn[T, U](
  val expr: Expression)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] { self =>

  /** From an untyped Column to a [[TypedColumn]]
    *
    * @param column a spark.sql Column
    * @param uencoder encoder of the resulting type U
    */
  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  /** Fall back to an untyped Column
    */
  def untyped: Column = new Column(expr)

  private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  private def equalsTo(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = withExpr {
    if (uencoder.nullable && uencoder.catalystRepr.typeName != "struct") EqualNullSafe(self.expr, other.expr)
    else EqualTo(self.expr, other.expr)
  }.typed

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(other: U): TypedColumn[T, Boolean] = equalsTo(lit(other))

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = equalsTo(other)

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def =!=(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = withExpr {
    Not(equalsTo(other).expr)
  }.typed

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= "a" )
    * }}}
    *
    * apache/spark
    */
  def =!=(other: U): TypedColumn[T, Boolean] = withExpr {
    Not(equalsTo(lit(other)).expr)
  }.typed

  /** True if the current expression is an Option and it's None.
    *
    * apache/spark
    */
  def isNone(implicit isOption: U <:< Option[_]): TypedColumn[T, Boolean] =
    equalsTo(lit[U,T](None.asInstanceOf[U]))

  /** True if the current expression is an Option and it's not None.
    *
    * apache/spark
    */
  def isNotNone(implicit isOption: U <:< Option[_]): TypedColumn[T, Boolean] = withExpr {
    Not(equalsTo(lit(None.asInstanceOf[U])).expr)
  }.typed

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(df('defaultValue)) )
    * }}}
    */
  def getOrElse[Out](default: TypedColumn[T, Out])(implicit isOption: U =:= Option[Out]): TypedColumn[T, Out] = withExpr {
    Coalesce(Seq(expr, default.expr))
  }.typed(default.uencoder)

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(defaultConstant) )
    * }}}
    */
  def getOrElse[Out: TypedEncoder](default: Out)(implicit isOption: U =:= Option[Out]): TypedColumn[T, Out] =
    getOrElse(lit[Out, T](default))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus(other: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] =
    self.untyped.plus(other.untyped).typed

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = plus(u)

  /** Sum of this expression (column) with a constant.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people('height) + 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def +(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.plus(u).typed

  /** Unary minus, i.e. negate the expression.
    * {{{
    *   // Select the amount column and negates all values.
    *   df.select( -df('amount) )
    * }}}
    *
    * apache/spark
    */
  def unary_-(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = (-self.untyped).typed

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] =
    self.untyped.minus(u.untyped).typed

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = minus(u)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people('height) - 1 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def -(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.minus(u).typed

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U], ct: ClassTag[U]): TypedColumn[T, U] = {
    if (ct.runtimeClass == BigDecimal(0).getClass) {
      // That's apparently the only way to get sound multiplication.
      // See https://issues.apache.org/jira/browse/SPARK-22036
      val dt = DecimalType(20, 14)
      self.untyped.cast(dt).multiply(u.untyped.cast(dt)).typed
    } else {
      self.untyped.multiply(u.untyped).typed
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
  def *(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U], tt: ClassTag[U]): TypedColumn[T, U] = multiply(u)

  /** Multiplication of this expression a constant.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.multiply(u).typed

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def divide[Out: TypedEncoder](other: TypedColumn[T, U])(implicit n: CatalystDivisible[U, Out]): TypedColumn[T, Out] =
    self.untyped.divide(other.untyped).typed

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param other another column of the same type
    * apache/spark
    */
  def /[Out](other: TypedColumn[T, U])
     (implicit
        n: CatalystDivisible[U, Out],
        e: TypedEncoder[Out]): TypedColumn[T, Out] = divide(other)

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def /(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, Double] = self.untyped.divide(u).typed

  //TODO: SCALADOC!!
  def desc(implicit catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Descending)
  }.typed

  def desc_nulls_first(implicit isOption: U <:< Option[_], catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Descending, NullsFirst, Set.empty)
  }.typed

  def desc_nulls_last(implicit isOption: U <:< Option[_], catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Descending, NullsLast, Set.empty)
  }.typed

  def asc(implicit catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Ascending)
  }.typed

  def asc_nulls_first(implicit isOption: U <:< Option[_], catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Ascending, NullsFirst, Set.empty)
  }.typed

  def asc_nulls_last(implicit isOption: U <:< Option[_], catalystRowOrdering: CatalystRowOrdered[U]): TypedColumn[T, U] = withExpr {
    SortOrder(expr, Ascending, NullsLast, Set.empty)
  }.typed

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = self.untyped.bitwiseAND(u).typed

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] =
    self.untyped.bitwiseAND(u.untyped).typed

  /**
    * Bitwise AND this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Int] & -1)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def &(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseAND(u)

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) & (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def &(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseAND(u)

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseOR(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = self.untyped.bitwiseOR(u).typed

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseOR(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] =
    self.untyped.bitwiseOR(u.untyped).typed

  /**
    * Bitwise OR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] | 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def |(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseOR(u)

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) | (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def |(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseOR(u)

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseXOR(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = self.untyped.bitwiseXOR(u).typed

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseXOR(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] =
    self.untyped.bitwiseXOR(u.untyped).typed

  /**
    * Bitwise XOR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] ^ 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def ^(u: U)(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseXOR(u)

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) ^ (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def ^(u: TypedColumn[T, U])(implicit n: CatalystBitwise[U]): TypedColumn[T, U] = bitwiseXOR(u)

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): TypedColumn[T, A] =
    self.untyped.cast(TypedEncoder[A].catalystRepr).typed

  /** Contains test.
    * {{{
    *   df.filter ( df.col('a).contains("foo") )
    * }}}
    */
  def contains(other: String)(implicit ev: U =:= String): TypedColumn[T, Boolean] =
    self.untyped.contains(other).typed

  /** Contains test.
    * {{{
    *   df.filter ( df.col('a).contains(df.col('b) )
    * }}}
    */
  def contains(other: TypedColumn[T, U])(implicit ev: U =:= String): TypedColumn[T, Boolean] =
    self.untyped.contains(other.untyped).typed

  /** Boolean AND.
    * {{{
    *   df.filter ( (df.col('a) === 1).and(df.col('b) > 5) )
    * }}}
    */
  def and(other: TypedColumn[T, Boolean]): TypedColumn[T, Boolean] =
    self.untyped.and(other.untyped).typed

  /** Boolean AND.
    * {{{
    *   df.filter ( df.col('a) === 1 && df.col('b) > 5)
    * }}}
    */
  def && (other: TypedColumn[T, Boolean]): TypedColumn[T, Boolean] =
    and(other)

  /** Boolean OR.
    * {{{
    *   df.filter ( (df.col('a) === 1).or(df.col('b) > 5) )
    * }}}
    */
  def or(other: TypedColumn[T, Boolean]): TypedColumn[T, Boolean] =
    self.untyped.or(other.untyped).typed

  /** Boolean OR.
    * {{{
    *   df.filter ( df.col('a) === 1 || df.col('b) > 5)
    * }}}
    */
  def || (other: TypedColumn[T, Boolean]): TypedColumn[T, Boolean] =
    or(other)
}

/** Expression used in `groupBy`-like constructions.
  *
  * @tparam T type of dataset
  * @tparam U type of column for `groupBy`
  */
sealed class TypedAggregate[T, U](val expr: Expression)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] {

  def this(column: Column)(implicit e: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }
}

object TypedColumn {
  /**
    * Evidence that type `T` has column `K` with type `V`.
    */
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

  implicit class OrderedTypedColumnSyntax[T, U: CatalystOrdered](col: TypedColumn[T, U]) {
    def <(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = (col.untyped < other.untyped).typed
    def <=(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = (col.untyped <= other.untyped).typed
    def >(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = (col.untyped > other.untyped).typed
    def >=(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = (col.untyped >= other.untyped).typed

    def <(other: U): TypedColumn[T, Boolean] = (col.untyped < lit(other)(col.uencoder).untyped).typed
    def <=(other: U): TypedColumn[T, Boolean] = (col.untyped <= lit(other)(col.uencoder).untyped).typed
    def >(other: U): TypedColumn[T, Boolean] = (col.untyped > lit(other)(col.uencoder).untyped).typed
    def >=(other: U): TypedColumn[T, Boolean] = (col.untyped >= lit(other)(col.uencoder).untyped).typed
  }
}
