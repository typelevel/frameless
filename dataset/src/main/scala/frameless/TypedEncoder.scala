package frameless

import java.math.BigInteger
import java.util.Date
import java.time.{ Duration, Instant, LocalDate, Period }
import java.sql.Timestamp
import scala.reflect.ClassTag
import FramelessInternals.UserDefinedType
import org.apache.spark.sql.catalyst.expressions.{ Expression, UnsafeArrayData, Literal }
import org.apache.spark.sql.catalyst.util.{
  ArrayBasedMapData,
  DateTimeUtils,
  GenericArrayData
}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import shapeless._
import shapeless.ops.hlist.IsHCons
import com.sparkutils.shim.expressions.{
  UnwrapOption2 => UnwrapOption,
  WrapOption2 => WrapOption,
  MapObjects5 => MapObjects,
  ExternalMapToCatalyst7 => ExternalMapToCatalyst
}
import frameless.{reflection => ScalaReflection}
import org.apache.spark.sql.shim.{
  StaticInvoke4 => StaticInvoke,
  NewInstance4 => NewInstance,
  Invoke5 => Invoke
}

abstract class TypedEncoder[T](
    implicit
    val classTag: ClassTag[T])
    extends Serializable {
  def nullable: Boolean

  def jvmRepr: DataType
  def catalystRepr: DataType

  /**
   * From Catalyst representation to T
   */
  def fromCatalyst(path: Expression): Expression

  /**
   * T to Catalyst representation
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
object TypedEncoder {
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
          Decimal.getClass,
          DecimalType.SYSTEM_DEFAULT,
          "apply",
          path :: Nil
        )

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
          Decimal.getClass,
          DecimalType.SYSTEM_DEFAULT,
          "apply",
          path :: Nil
        )

      def fromCatalyst(path: Expression): Expression =
        Invoke(path, "toJavaBigDecimal", jvmRepr)

      override def toString: String = "javaBigDecimalEncoder"
    }

  implicit val bigIntEncoder: TypedEncoder[BigInt] = new TypedEncoder[BigInt] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ScalaReflection.dataTypeFor[BigInt]
    def catalystRepr: DataType = DecimalType(DecimalType.MAX_PRECISION, 0)

    def toCatalyst(path: Expression): Expression =
      StaticInvoke(
        Decimal.getClass,
        catalystRepr,
        "apply",
        path :: Nil
      )

    def fromCatalyst(path: Expression): Expression =
      Invoke(path, "toScalaBigInt", jvmRepr)

    override def toString: String = "bigIntEncoder"
  }

  implicit val javaBigIntEncoder: TypedEncoder[BigInteger] =
    new TypedEncoder[BigInteger] {
      def nullable: Boolean = false

      def jvmRepr: DataType = ScalaReflection.dataTypeFor[BigInteger]
      def catalystRepr: DataType = DecimalType(DecimalType.MAX_PRECISION, 0)

      def toCatalyst(path: Expression): Expression =
        StaticInvoke(
          Decimal.getClass,
          catalystRepr,
          "apply",
          path :: Nil
        )

      def fromCatalyst(path: Expression): Expression =
        Invoke(path, "toJavaBigInteger", jvmRepr)

      override def toString: String = "javaBigIntEncoder"
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

  implicit val timestampEncoder: TypedEncoder[Timestamp] =
    new TypedEncoder[Timestamp] {
      def nullable: Boolean = false

      def jvmRepr: DataType = ScalaReflection.dataTypeFor[Timestamp]
      def catalystRepr: DataType = TimestampType

      def toCatalyst(path: Expression): Expression =
        StaticInvoke(
          DateTimeUtils.getClass,
          TimestampType,
          "fromJavaTimestamp",
          path :: Nil,
          returnNullable = false
        )

      def fromCatalyst(path: Expression): Expression =
        StaticInvoke(
          staticObject = DateTimeUtils.getClass,
          dataType = jvmRepr,
          functionName = "toJavaTimestamp",
          arguments = path :: Nil,
          propagateNull = true
        )

      override def toString: String = "timestampEncoder"
    }

  implicit val dateEncoder: TypedEncoder[Date] = new TypedEncoder[Date] {
    def nullable: Boolean = false

    def jvmRepr: DataType = ScalaReflection.dataTypeFor[Date]
    def catalystRepr: DataType = TimestampType

    private val instantRepr = ScalaReflection.dataTypeFor[Instant]

    def toCatalyst(path: Expression): Expression =
      timeInstant.toCatalyst(Invoke(path, "toInstant", instantRepr))

    def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        staticObject = classOf[Date],
        dataType = jvmRepr,
        functionName = "from",
        arguments = timeInstant.fromCatalyst(path) :: Nil,
        propagateNull = true
      )

    override def toString: String = "dateEncoder"
  }

  implicit val sqlDateEncoder: TypedEncoder[java.sql.Date] =
    new TypedEncoder[java.sql.Date] {
      def nullable: Boolean = false

      def jvmRepr: DataType = ScalaReflection.dataTypeFor[java.sql.Date]
      def catalystRepr: DataType = DateType

      def toCatalyst(path: Expression): Expression =
        StaticInvoke(
          staticObject = DateTimeUtils.getClass,
          dataType = catalystRepr,
          functionName = "fromJavaDate",
          arguments = path :: Nil,
          propagateNull = true
        )

      private val localDateRepr = ScalaReflection.dataTypeFor[LocalDate]

      def fromCatalyst(path: Expression): Expression = {
        val toLocalDate = StaticInvoke(
          staticObject = DateTimeUtils.getClass,
          dataType = localDateRepr,
          functionName = "daysToLocalDate",
          arguments = path :: Nil,
          propagateNull = true
        )

        StaticInvoke(
          staticObject = classOf[java.sql.Date],
          dataType = jvmRepr,
          functionName = "valueOf",
          arguments = toLocalDate :: Nil,
          propagateNull = true
        )
      }

      override def toString: String = "sqlDateEncoder"
    }

  implicit val sqlTimestamp: TypedEncoder[SQLTimestamp] =
    new TypedEncoder[SQLTimestamp] {
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
        returnNullable = false
      )

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
  implicit val timeDurationInjection: Injection[Duration, Long] =
    Injection(_.toMillis, Duration.ofMillis)

  // YearMonthIntervalType
  implicit val timePeriodInjection: Injection[Period, Int] =
    Injection(_.getDays, Period.ofDays)

  implicit val timePeriodEncoder: TypedEncoder[Period] =
    TypedEncoder.usingInjection

  implicit val timeDurationEncoder: TypedEncoder[Duration] =
    TypedEncoder.usingInjection

  implicit def arrayEncoder[T: ClassTag](
      implicit
      i0: Lazy[RecordFieldEncoder[T]]
    ): TypedEncoder[Array[T]] =
    new TypedEncoder[Array[T]] {
      private lazy val encodeT = i0.value.encoder

      def nullable: Boolean = false

      lazy val jvmRepr: DataType = i0.value.jvmRepr match {
        case ByteType => BinaryType
        case _        => FramelessInternals.objectTypeFor[Array[T]]
      }

      lazy val catalystRepr: DataType = i0.value.jvmRepr match {
        case ByteType => BinaryType
        case _        => ArrayType(encodeT.catalystRepr, encodeT.nullable)
      }

      def toCatalyst(path: Expression): Expression = {
        val enc = i0.value

        enc.jvmRepr match {
          case IntegerType | LongType | DoubleType | FloatType | ShortType |
              BooleanType =>
            StaticInvoke(
              classOf[UnsafeArrayData],
              catalystRepr,
              "fromPrimitiveArray",
              path :: Nil
            )

          case ByteType => path

          case _ =>
            MapObjects(enc.toCatalyst, path, enc.jvmRepr, encodeT.nullable)
        }
      }

      def fromCatalyst(path: Expression): Expression =
        encodeT.jvmRepr match {
          case IntegerType => Invoke(path, "toIntArray", jvmRepr)
          case LongType    => Invoke(path, "toLongArray", jvmRepr)
          case DoubleType  => Invoke(path, "toDoubleArray", jvmRepr)
          case FloatType   => Invoke(path, "toFloatArray", jvmRepr)
          case ShortType   => Invoke(path, "toShortArray", jvmRepr)
          case BooleanType => Invoke(path, "toBooleanArray", jvmRepr)

          case ByteType => path

          case _ =>
            Invoke(
              MapObjects(
                i0.value.fromCatalyst,
                path,
                encodeT.catalystRepr,
                encodeT.nullable
              ),
              "array",
              jvmRepr
            )
        }

      override def toString: String = s"arrayEncoder($jvmRepr)"
    }

  implicit def collectionEncoder[C[X] <: Seq[X], T](
      implicit
      i0: Lazy[RecordFieldEncoder[T]],
      i1: ClassTag[C[T]]
    ): TypedEncoder[C[T]] = new TypedEncoder[C[T]] {
    private lazy val encodeT = i0.value.encoder

    def nullable: Boolean = false

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[C[T]](i1)

    def catalystRepr: DataType =
      ArrayType(encodeT.catalystRepr, encodeT.nullable)

    def toCatalyst(path: Expression): Expression = {
      val enc = i0.value

      if (ScalaReflection.isNativeType(enc.jvmRepr)) {
        NewInstance(classOf[GenericArrayData], path :: Nil, catalystRepr)
      } else {
        MapObjects(enc.toCatalyst, path, enc.jvmRepr, encodeT.nullable)
      }
    }

    def fromCatalyst(path: Expression): Expression =
      MapObjects(
        i0.value.fromCatalyst,
        path,
        encodeT.catalystRepr,
        encodeT.nullable,
        Some(i1.runtimeClass) // This will cause MapObjects to build a collection of type C[_] directly
      )

    override def toString: String = s"collectionEncoder($jvmRepr)"
  }

  /**
   * @param i1 implicit lazy `RecordFieldEncoder[T]` to encode individual elements of the set.
   * @param i2 implicit `ClassTag[Set[T]]` to provide runtime information about the set type.
   * @tparam T the element type of the set.
   * @return a `TypedEncoder` instance for `Set[T]`.
   */
  implicit def setEncoder[T](
      implicit
      i1: shapeless.Lazy[RecordFieldEncoder[T]],
      i2: ClassTag[Set[T]]
    ): TypedEncoder[Set[T]] = {
    implicit val inj: Injection[Set[T], Seq[T]] = Injection(_.toSeq, _.toSet)

    TypedEncoder.usingInjection
  }

  /**
   * @tparam A the key type
   * @tparam B the value type
   * @param i0 the keys encoder
   * @param i1 the values encoder
   */
  implicit def mapEncoder[A: NotCatalystNullable, B](
      implicit
      i0: Lazy[RecordFieldEncoder[A]],
      i1: Lazy[RecordFieldEncoder[B]]
    ): TypedEncoder[Map[A, B]] = new TypedEncoder[Map[A, B]] {
    def nullable: Boolean = false

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[Map[A, B]]

    private lazy val encodeA = i0.value.encoder
    private lazy val encodeB = i1.value.encoder

    lazy val catalystRepr: DataType =
      MapType(encodeA.catalystRepr, encodeB.catalystRepr, encodeB.nullable)

    def fromCatalyst(path: Expression): Expression = {
      val keyArrayType = ArrayType(encodeA.catalystRepr, containsNull = false)

      val keyData = Invoke(
        MapObjects(
          i0.value.fromCatalyst,
          Invoke(path, "keyArray", keyArrayType),
          encodeA.catalystRepr
        ),
        "array",
        FramelessInternals.objectTypeFor[Array[Any]]
      )

      val valueArrayType = ArrayType(encodeB.catalystRepr, encodeB.nullable)

      val valueData = Invoke(
        MapObjects(
          i1.value.fromCatalyst,
          Invoke(path, "valueArray", valueArrayType),
          encodeB.catalystRepr
        ),
        "array",
        FramelessInternals.objectTypeFor[Array[Any]]
      )

      StaticInvoke(
        ArrayBasedMapData.getClass,
        jvmRepr,
        "toScalaMap",
        keyData :: valueData :: Nil
      )
    }

    def toCatalyst(path: Expression): Expression = {
      val encA = i0.value
      val encB = i1.value

      ExternalMapToCatalyst(
        path,
        encA.jvmRepr,
        encA.toCatalyst,
        false,
        encB.jvmRepr,
        encB.toCatalyst,
        encodeB.nullable
      )
    }

    override def toString = s"mapEncoder($jvmRepr)"
  }

  implicit def optionEncoder[A](
      implicit
      underlying: TypedEncoder[A]
    ): TypedEncoder[Option[A]] =
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
                ScalaReflection.dataTypeFor[java.lang.Integer],
                path
              ),
              "intValue",
              IntegerType
            )

          case LongType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Long], path),
              "longValue",
              LongType
            )

          case DoubleType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Double], path),
              "doubleValue",
              DoubleType
            )

          case FloatType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Float], path),
              "floatValue",
              FloatType
            )

          case ShortType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Short], path),
              "shortValue",
              ShortType
            )

          case ByteType =>
            Invoke(
              UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Byte], path),
              "byteValue",
              ByteType
            )

          case BooleanType =>
            Invoke(
              UnwrapOption(
                ScalaReflection.dataTypeFor[java.lang.Boolean],
                path
              ),
              "booleanValue",
              BooleanType
            )

          case _ =>
            underlying.toCatalyst(UnwrapOption(underlying.jvmRepr, path))
        }
      }

      def fromCatalyst(path: Expression): Expression =
        WrapOption(underlying.fromCatalyst(path), underlying.jvmRepr)
    }

  /** Encodes things using injection if there is one defined */
  implicit def usingInjection[A: ClassTag, B](
      implicit
      inj: Injection[A, B],
      trb: TypedEncoder[B]
    ): TypedEncoder[A] =
    new TypedEncoder[A] {
      def nullable: Boolean = trb.nullable
      def jvmRepr: DataType = FramelessInternals.objectTypeFor[A](classTag)
      def catalystRepr: DataType = trb.catalystRepr

      def fromCatalyst(path: Expression): Expression = {
        val bexpr = trb.fromCatalyst(path)
        Invoke(Literal.fromObject(inj), "invert", jvmRepr, Seq(bexpr))
      }

      def toCatalyst(path: Expression): Expression =
        trb.toCatalyst(
          Invoke(Literal.fromObject(inj), "apply", trb.jvmRepr, Seq(path))
        )
    }

  /** Encodes things as records if there is no Injection defined */
  implicit def usingDerivation[F, G <: HList, H <: HList](
      implicit
      i0: LabelledGeneric.Aux[F, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons[H],
      i3: Lazy[RecordEncoderFields[H]],
      i4: Lazy[NewInstanceExprs[G]],
      i5: ClassTag[F]
    ): TypedEncoder[F] = new RecordEncoder[F, G, H]

  /** Encodes things using a Spark SQL's User Defined Type (UDT) if there is one defined in implicit */
  implicit def usingUserDefinedType[
      A >: Null: UserDefinedType: ClassTag
    ]: TypedEncoder[A] = {
    val udt = implicitly[UserDefinedType[A]]
    val udtInstance =
      NewInstance(udt.getClass, Nil, dataType = ObjectType(udt.getClass))

    new TypedEncoder[A] {
      def nullable: Boolean = false
      def jvmRepr: DataType = ObjectType(udt.userClass)
      def catalystRepr: DataType = udt

      def toCatalyst(path: Expression): Expression =
        Invoke(udtInstance, "serialize", udt, Seq(path))

      def fromCatalyst(path: Expression): Expression =
        Invoke(udtInstance, "deserialize", ObjectType(udt.userClass), Seq(path))
    }
  }

  object injections extends InjectionEnum
}
