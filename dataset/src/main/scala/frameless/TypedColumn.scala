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
) extends AbstractTypedColumn[T,U](expr) {

  type TC[A] = TypedColumn[T,A]

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  override def typed[U1: TypedEncoder](c: Column): TypedColumn[T, U1] = c.typedColumn
  override def lit[U1: TypedEncoder](c: U1): TypedColumn[T,U1] = flit(c)
}

/** Expression used in `agg`-like constructions.
  */
sealed class TypedAggregate[T, U](expr: Expression)(
  implicit val uenc: TypedEncoder[U]
) extends AbstractTypedColumn[T,U](expr) {

  type TC[A] = TypedAggregate[T, A]

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  override def typed[U1: TypedEncoder](c: Column): TypedAggregate[T,U1] = c.typedAggregate
  override def lit[U1: TypedEncoder](c: U1): TypedAggregate[T,U1] = litAggr(c)
}

/** Generic representation of a typed column. A typed column can either be a [[TypedAggregate]] or
  * a [[frameless.TypedColumn]].
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * @tparam T type of dataset
  * @tparam U type of column
  */
abstract class AbstractTypedColumn[T, U](val expr: Expression)(
  implicit val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] { self =>

  type TC[A] <: AbstractTypedColumn[T, A]

  /** Fall back to an untyped Column
    */
  def untyped: Column = new Column(expr)

  private def equalsTo(other: TC[U]): TC[Boolean] = typed {
    if (uencoder.nullable && uencoder.catalystRepr.typeName != "struct") EqualNullSafe(self.expr, other.expr)
    else EqualTo(self.expr, other.expr)
  }

  /** Creates a typed column of either TypedColumn or TypedAggregate from an expression.
    */
  protected def typed[U1: TypedEncoder](e: Expression): TC[U1] = typed(new Column(e))

  /** Creates a typed column of either TypedColumn or TypedAggregate.
    */
  protected def typed[U1: TypedEncoder](c: Column): TC[U1]

  /** Creates a typed column of either TypedColumn or TypedAggregate.
    */
  protected def lit[U1: TypedEncoder](c: U1): TC[U1]

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(other: U): TC[Boolean] = equalsTo(lit(other))

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===(other: TC[U]): TC[Boolean] = equalsTo(other)

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def =!=(other: TC[U]): TC[Boolean] = typed(Not(equalsTo(other).expr))

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= "a" )
    * }}}
    *
    * apache/spark
    */
  def =!=(other: U): TC[Boolean] = typed(Not(equalsTo(lit(other)).expr))

  /** True if the current expression is an Option and it's None.
    *
    * apache/spark
    */
  def isNone(implicit isOption: U <:< Option[_]): TC[Boolean] =
    equalsTo(lit[U](None.asInstanceOf[U]))

  /** True if the current expression is an Option and it's not None.
    *
    * apache/spark
    */
  def isNotNone(implicit isOption: U <:< Option[_]): TC[Boolean] =
    typed(Not(equalsTo(lit(None.asInstanceOf[U])).expr))

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(df('defaultValue)) )
    * }}}
    */
  def getOrElse[Out](default: TC[Out])(implicit isOption: U =:= Option[Out]): TC[Out] =
    typed(Coalesce(Seq(expr, default.expr)))(default.uencoder)

  /** Convert an Optional column by providing a default value
    * {{{
    *   df( df('opt).getOrElse(defaultConstant) )
    * }}}
    */
  def getOrElse[Out: TypedEncoder](default: Out)(implicit isOption: U =:= Option[Out]): TC[Out] =
    getOrElse(lit[Out](default))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus(other: TC[U])(implicit n: CatalystNumeric[U]): TC[U] =
    typed(self.untyped.plus(other.untyped))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +(u: TC[U])(implicit n: CatalystNumeric[U]): TC[U] = plus(u)

  /** Sum of this expression (column) with a constant.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people('height) + 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def +(u: U)(implicit n: CatalystNumeric[U]): TC[U] = typed(self.untyped.plus(u))

  /** Unary minus, i.e. negate the expression.
    * {{{
    *   // Select the amount column and negates all values.
    *   df.select( -df('amount) )
    * }}}
    *
    * apache/spark
    */
  def unary_-(implicit n: CatalystNumeric[U]): TC[U] = typed(-self.untyped)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus(u: TC[U])(implicit n: CatalystNumeric[U]): TC[U] = typed(self.untyped.minus(u.untyped))

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -(u: TC[U])(implicit n: CatalystNumeric[U]): TC[U] = minus(u)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people('height) - 1 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def -(u: U)(implicit n: CatalystNumeric[U]): TC[U] = typed(self.untyped.minus(u))

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply(u: TC[U])(implicit n: CatalystNumeric[U], ct: ClassTag[U]): TC[U] = typed {
    if (ct.runtimeClass == BigDecimal(0).getClass) {
      // That's apparently the only way to get sound multiplication.
      // See https://issues.apache.org/jira/browse/SPARK-22036
      val dt = DecimalType(20, 14)
      self.untyped.cast(dt).multiply(u.untyped.cast(dt))
    } else {
      self.untyped.multiply(u.untyped)
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
  def *(u: TC[U])(implicit n: CatalystNumeric[U], tt: ClassTag[U]): TC[U] = multiply(u)

  /** Multiplication of this expression a constant.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: U)(implicit n: CatalystNumeric[U]): TC[U] = typed(self.untyped.multiply(u))

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
  def divide[Out: TypedEncoder](other: TC[U])(implicit n: CatalystDivisible[U, Out]): TC[Out] =
    typed(self.untyped.divide(other.untyped))

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
  def /[Out](other: TC[U])
     (implicit
        n: CatalystDivisible[U, Out],
        e: TypedEncoder[Out]): TC[Out] = divide(other)

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
  def /(u: U)(implicit n: CatalystNumeric[U]): TC[Double] = typed(self.untyped.divide(u))

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND(u: U)(implicit n: CatalystBitwise[U]): TC[U] =
    typed(self.untyped.bitwiseAND(u))

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseAND (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseAND(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] =
    typed(self.untyped.bitwiseAND(u.untyped))

  /**
    * Bitwise AND this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Int] & -1)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def &(u: U)(implicit n: CatalystBitwise[U]): TC[U] = bitwiseAND(u)

  /**
    * Bitwise AND this expression and another expression.
    * {{{
    *   df.select(df.col('colA) & (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def &(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] = bitwiseAND(u)

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseOR(u: U)(implicit n: CatalystBitwise[U]): TC[U] = typed(self.untyped.bitwiseOR(u))

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseOR(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] =
    typed(self.untyped.bitwiseOR(u.untyped))

  /**
    * Bitwise OR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] | 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def |(u: U)(implicit n: CatalystBitwise[U]): TC[U] = bitwiseOR(u)

  /**
    * Bitwise OR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) | (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def |(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] = bitwiseOR(u)

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseXOR(u: U)(implicit n: CatalystBitwise[U]): TC[U] =
    typed(self.untyped.bitwiseXOR(u))

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) bitwiseXOR (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def bitwiseXOR(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] =
    typed(self.untyped.bitwiseXOR(u.untyped))

  /**
    * Bitwise XOR this expression and another expression (of same type).
    * {{{
    *   df.select(df.col('colA).cast[Long] ^ 1L)
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def ^(u: U)(implicit n: CatalystBitwise[U]): TC[U] = bitwiseXOR(u)

  /**
    * Bitwise XOR this expression and another expression.
    * {{{
    *   df.select(df.col('colA) ^ (df.col('colB)))
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def ^(u: TC[U])(implicit n: CatalystBitwise[U]): TC[U] = bitwiseXOR(u)

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): TC[A] =
    typed(self.untyped.cast(TypedEncoder[A].catalystRepr))

  /** Contains test.
    * {{{
    *   df.filter ( df.col('a).contains("foo") )
    * }}}
    */
  def contains(other: String)(implicit ev: U =:= String): TC[Boolean] =
    typed(self.untyped.contains(other))

  /** Contains test.
    * {{{
    *   df.filter ( df.col('a).contains(df.col('b) )
    * }}}
    */
  def contains(other: TC[U])(implicit ev: U =:= String): TC[Boolean] =
    typed(self.untyped.contains(other.untyped))

  /** Boolean AND.
    * {{{
    *   df.filter ( (df.col('a) === 1).and(df.col('b) > 5) )
    * }}}
    */
  def and(other: TC[Boolean]): TC[Boolean] =
    typed(self.untyped.and(other.untyped))

  /** Boolean AND.
    * {{{
    *   df.filter ( df.col('a) === 1 && df.col('b) > 5)
    * }}}
    */
  def && (other: TC[Boolean]): TC[Boolean] = and(other)

  /** Boolean OR.
    * {{{
    *   df.filter ( (df.col('a) === 1).or(df.col('b) > 5) )
    * }}}
    */
  def or(other: TC[Boolean]): TC[Boolean] =
    typed(self.untyped.or(other.untyped))

  /** Boolean OR.
    * {{{
    *   df.filter ( df.col('a) === 1 || df.col('b) > 5)
    * }}}
    */
  def || (other: TC[Boolean]): TC[Boolean] = or(other)

  /**
    * Less than.
    * {{{
    *   // The following selects people younger than the maxAge column.
    *   df.select( df('age) < df('maxAge) )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def <(u: TC[U])(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped < u.untyped)

  /**
    * Less than or equal to.
    * {{{
    *   // The following selects people younger or equal than the maxAge column.
    *   df.select( df('age) <= df('maxAge)
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def <=(u: TC[U])(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped <= u.untyped)

  /**
    * Greater than.
    * {{{
    *   // The following selects people older than the maxAge column.
    *   df.select( df('age) > df('maxAge) )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >(u: TC[U])(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped > u.untyped)

  /**
    * Greater than or equal.
    * {{{
    *   // The following selects people older or equal than the maxAge column.
    *   df.select( df('age) >= df('maxAge) )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >=(u: TC[U])(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped >= u.untyped)

  /**
    * Less than.
    * {{{
    *   // The following selects people younger than 21.
    *   df.select( df('age) < 21 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def <(u: U)(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped < lit(u)(self.uencoder).untyped)

  /**
    * Less than or equal to.
    * {{{
    *   // The following selects people younger than 22.
    *   df.select( df('age) <= 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def <=(u: U)(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped <= lit(u)(self.uencoder).untyped)

  /**
    * Greater than.
    * {{{
    *   // The following selects people older than 21.
    *   df.select( df('age) > 21 )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >(u: U)(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped > lit(u)(self.uencoder).untyped)

  /**
    * Greater than or equal.
    * {{{
    *   // The following selects people older than 20.
    *   df.select( df('age) >= 21 )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def >=(u: U)(implicit canOrder: CatalystOrdered[U]): TC[Boolean] =
    typed(self.untyped >= lit(u)(self.uencoder).untyped)
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
}
