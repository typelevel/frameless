package frameless

import java.time.{Duration, Instant, Period}
import scala.reflect.ClassTag

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.FramelessInternals.UserDefinedType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class TypedEncoder[T](implicit val classTag: ClassTag[T]) extends Serializable {
  def nullable: Boolean

  def jvmRepr: DataType
  def catalystRepr: DataType

  /** From Catalyst representation to T
    */
  def fromCatalyst(path: Expression): Expression

  /** T to Catalyst representation
    */
  def toCatalyst(path: Expression): Expression
}

// Waiting on scala 2.12
// @annotation.implicitAmbiguous(msg =
// """TypedEncoder[${T}] can be obtained from automatic type class derivation, using the implicit Injection[${T}, ?] or using the implicit UserDefinedType[${T}] in scope.
// To desambigious this resolution you need to either:
//   - Remove the implicit Injection[${T}, ?] from scope
//   - Remove the implicit UserDefinedType[${T}] from scope
//   - import TypedEncoder.usingInjection
//   - import TypedEncoder.usingDerivation
//   - import TypedEncoder.usingUserDefinedType
// """)
object TypedEncoder extends TypedEncoderCompat {
  def apply[T: TypedEncoder]: TypedEncoder[T] = implicitly[TypedEncoder[T]]

  implicit val stringEncoder: TypedEncoder[String] = new TypedEncoder[String] {
    def nullable: Boolean = false

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[String]
    def catalystRepr: DataType = StringType

    def toCatalyst(path: Expression): Expression =
      StaticInvoke(classOf[UTF8String], catalystRepr, "fromString", path :: Nil)

    def fromCatalyst(path: Expression): Expression =
      Invoke(path, "toString", jvmRepr)

    override val toString = "stringEncoder"
  }

  implicit val booleanEncoder: TypedEncoder[Boolean] =
    new TypedEncoder[Boolean] {
      def nullable: Boolean = false

      def jvmRepr: DataType = BooleanType
      def catalystRepr: DataType = BooleanType

      def toCatalyst(path: Expression): Expression = path
      def fromCatalyst(path: Expression): Expression = path
    }

  implicit val intEncoder: TypedEncoder[Int] = new TypedEncoder[Int] {
    def nullable: Boolean = false

    def jvmRepr: DataType = IntegerType
    def catalystRepr: DataType = IntegerType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path

    override def toString = "intEncoder"
  }

  implicit val longEncoder: TypedEncoder[Long] = new TypedEncoder[Long] {
    def nullable: Boolean = false

    def jvmRepr: DataType = LongType
    def catalystRepr: DataType = LongType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path
  }

  implicit val shortEncoder: TypedEncoder[Short] = new TypedEncoder[Short] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ShortType
    def catalystRepr: DataType = ShortType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path
  }

  implicit val charEncoder: TypedEncoder[Char] = new TypedEncoder[Char] {
    // tricky because while Char is primitive type, Spark doesn't support it
    implicit val charAsString: Injection[java.lang.Character, String] =
      new Injection[java.lang.Character, String] {
        def apply(a: java.lang.Character): String = String.valueOf(a)

        def invert(b: String): java.lang.Character = {
          require(b.length == 1)
          b.charAt(0)
        }
      }

    val underlying = usingInjection[java.lang.Character, String]

    def nullable: Boolean = false

    // this line fixes underlying encoder
    def jvmRepr: DataType =
      FramelessInternals.objectTypeFor[java.lang.Character]

    def catalystRepr: DataType = StringType

    def toCatalyst(path: Expression): Expression = underlying.toCatalyst(path)

    def fromCatalyst(path: Expression): Expression =
      underlying.fromCatalyst(path)
  }

  implicit val byteEncoder: TypedEncoder[Byte] = new TypedEncoder[Byte] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ByteType
    def catalystRepr: DataType = ByteType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path
  }

  implicit val floatEncoder: TypedEncoder[Float] = new TypedEncoder[Float] {
    def nullable: Boolean = false

    def jvmRepr: DataType = FloatType
    def catalystRepr: DataType = FloatType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path
  }

  implicit val doubleEncoder: TypedEncoder[Double] = new TypedEncoder[Double] {
    def nullable: Boolean = false

    def jvmRepr: DataType = DoubleType
    def catalystRepr: DataType = DoubleType

    def toCatalyst(path: Expression): Expression = path
    def fromCatalyst(path: Expression): Expression = path
  }

  implicit val bigDecimalEncoder: TypedEncoder[BigDecimal] =
    new TypedEncoder[BigDecimal] {
      def nullable: Boolean = false

      def jvmRepr: DataType = ScalaReflection.dataTypeFor[BigDecimal]
      def catalystRepr: DataType = DecimalType.SYSTEM_DEFAULT

      def toCatalyst(path: Expression): Expression =
        StaticInvoke(
          Decimal.getClass, DecimalType.SYSTEM_DEFAULT, "apply", path :: Nil)

      def fromCatalyst(path: Expression): Expression =
        Invoke(path, "toBigDecimal", jvmRepr)

      override def toString: String = "bigDecimalEncoder"
    }

  implicit val javaBigDecimalEncoder: TypedEncoder[java.math.BigDecimal] =
    new TypedEncoder[java.math.BigDecimal] {
      def nullable: Boolean = false

      def jvmRepr: DataType = ScalaReflection.dataTypeFor[java.math.BigDecimal]
      def catalystRepr: DataType = DecimalType.SYSTEM_DEFAULT

      def toCatalyst(path: Expression): Expression =
        StaticInvoke(
          Decimal.getClass, DecimalType.SYSTEM_DEFAULT, "apply", path :: Nil)

      def fromCatalyst(path: Expression): Expression =
        Invoke(path, "toJavaBigDecimal", jvmRepr)

      override def toString: String = "javaBigDecimalEncoder"
    }

  implicit val sqlDate: TypedEncoder[SQLDate] = new TypedEncoder[SQLDate] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ScalaReflection.dataTypeFor[SQLDate]
    def catalystRepr: DataType = DateType

    def toCatalyst(path: Expression): Expression =
      Invoke(path, "days", DateType)

    def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        staticObject = SQLDate.getClass,
        dataType = jvmRepr,
        functionName = "apply",
        arguments = path :: Nil,
        propagateNull = true
      )
  }

  implicit val sqlTimestamp: TypedEncoder[SQLTimestamp] = new TypedEncoder[SQLTimestamp] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ScalaReflection.dataTypeFor[SQLTimestamp]
    def catalystRepr: DataType = TimestampType

    def toCatalyst(path: Expression): Expression =
      Invoke(path, "us", TimestampType)

    def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        staticObject = SQLTimestamp.getClass,
        dataType = jvmRepr,
        functionName = "apply",
        arguments = path :: Nil,
        propagateNull = true
      )
  }

  /** java.time Encoders, Spark uses https://github.com/apache/spark/blob/v3.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala for encoding / decoding. */
  implicit val timeInstant: TypedEncoder[Instant] = new TypedEncoder[Instant] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ScalaReflection.dataTypeFor[Instant]
    def catalystRepr: DataType = TimestampType

    def toCatalyst(path: Expression): Expression =
      StaticInvoke(
        DateTimeUtils.getClass,
        TimestampType,
        "instantToMicros",
        path :: Nil,
        returnNullable = false)

    def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        staticObject = DateTimeUtils.getClass,
        dataType = jvmRepr,
        functionName = "microsToInstant",
        arguments = path :: Nil,
        propagateNull = true
      )
  }

  /**
    * DayTimeIntervalType and YearMonthIntervalType in Spark 3.2.0.
    * We maintain Spark 3.x cross compilation and handle Duration and Period as an injections to be compatible with Spark versions < 3.2
    * See
    *  * https://github.com/apache/spark/blob/v3.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/IntervalUtils.scala#L1031-L1047
    *  * https://github.com/apache/spark/blob/v3.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/IntervalUtils.scala#L1075-L1087
    */
  // DayTimeIntervalType
  implicit val timeDurationInjection: Injection[Duration, Long] = Injection(_.toMillis, Duration.ofMillis)
  // YearMonthIntervalType
  implicit val timePeriodInjection: Injection[Period, Int] = Injection(_.getDays, Period.ofDays)
  implicit val timePeriodEncoder: TypedEncoder[Period] = TypedEncoder.usingInjection
  implicit val timeDurationEncoder: TypedEncoder[Duration] = TypedEncoder.usingInjection

  implicit def optionEncoder[A](implicit underlying: TypedEncoder[A]): TypedEncoder[Option[A]] =
    new TypedEncoder[Option[A]] {
      def nullable: Boolean = true

      def jvmRepr: DataType =
        FramelessInternals.objectTypeFor[Option[A]](classTag)

      def catalystRepr: DataType = underlying.catalystRepr

      def toCatalyst(path: Expression): Expression = {
        // for primitive types we must manually unbox the value of the object
        underlying.jvmRepr match {
          case IntegerType =>
            Invoke(
              UnwrapOption(
                ScalaReflection.dataTypeFor[java.lang.Integer], path),
              "intValue",
              IntegerType)

          case LongType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Long], path),
              "longValue",
              LongType)

          case DoubleType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Double], path),
              "doubleValue",
              DoubleType)

          case FloatType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Float], path),
              "floatValue",
              FloatType)

          case ShortType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Short], path),
              "shortValue",
              ShortType)

          case ByteType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Byte], path),
              "byteValue",
              ByteType)

          case BooleanType =>
            Invoke(
              UnwrapOption(
                ScalaReflection.dataTypeFor[java.lang.Boolean], path),
              "booleanValue",
              BooleanType)

          case _ => underlying.toCatalyst(
            UnwrapOption(underlying.jvmRepr, path))
        }
      }

    def fromCatalyst(path: Expression): Expression =
      WrapOption(underlying.fromCatalyst(path), underlying.jvmRepr)
  }

  /** Encodes things using a Spark SQL's User Defined Type (UDT) if there is one defined in implicit */
  implicit def usingUserDefinedType[A >: Null : UserDefinedType : ClassTag]: TypedEncoder[A] = {
    val udt = implicitly[UserDefinedType[A]]
    val udtInstance = NewInstance(
      udt.getClass, Nil, dataType = ObjectType(udt.getClass))

    new TypedEncoder[A] {
      def nullable: Boolean = false
      def jvmRepr: DataType = ObjectType(udt.userClass)
      def catalystRepr: DataType = udt

      def toCatalyst(path: Expression): Expression = Invoke(udtInstance, "serialize", udt, Seq(path))

      def fromCatalyst(path: Expression): Expression =
        Invoke(udtInstance, "deserialize", ObjectType(udt.userClass), Seq(path))
    }
  }

  object injections extends InjectionEnum
}
