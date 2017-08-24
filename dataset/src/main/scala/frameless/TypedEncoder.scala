package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import shapeless._
import scala.reflect.ClassTag
import frameless.udt.Udt

abstract class TypedEncoder[T](implicit val classTag: ClassTag[T]) extends Serializable {
  def nullable: Boolean

  def sourceDataType: DataType
  def targetDataType: DataType

  /** From Catalyst representation to T
    */
  def constructorFor(path: Expression): Expression

  /** T to Catalyst representation
    */
  def extractorFor(path: Expression): Expression
}

// Waiting on scala 2.12
// @annotation.implicitAmbiguous(msg =
// """TypedEncoder[${T}] can be obtained from automatic type class derivation, using the implicit Injection[${T}, ?] or using the implicit UDT[${T]] in scope.
// To desambigious this resolution you need to either:
//   - Remove the implicit Injection[${T}, ?] from scope
//   - Remove the implicit UDT[${T]] from scope
//   - import TypedEncoder.usingInjection
//   - import TypedEncoder.usingDerivation
//   - import TypedEncoder.usingUDT
// """)
object TypedEncoder {
  def apply[T: TypedEncoder]: TypedEncoder[T] = implicitly[TypedEncoder[T]]

  implicit val unitEncoder: TypedEncoder[Unit] = new TypedEncoder[Unit] {
    def nullable: Boolean = true

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[Unit]
    def targetDataType: DataType = NullType

    def constructorFor(path: Expression): Expression = Literal.create((), sourceDataType)
    def extractorFor(path: Expression): Expression = Literal.create(null, targetDataType)
  }

  implicit val stringEncoder: TypedEncoder[String] = new TypedEncoder[String] {
    def nullable: Boolean = true

    def sourceDataType: DataType = FramelessInternals.objectTypeFor[String]
    def targetDataType: DataType = StringType

    def extractorFor(path: Expression): Expression =
      StaticInvoke(classOf[UTF8String], targetDataType, "fromString", path :: Nil)

    def constructorFor(path: Expression): Expression =
      Invoke(path, "toString", sourceDataType)
  }

  implicit val booleanEncoder: TypedEncoder[Boolean] = new TypedEncoder[Boolean] {
    def nullable: Boolean = false

    def sourceDataType: DataType = BooleanType
    def targetDataType: DataType = BooleanType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val intEncoder: TypedEncoder[Int] = new TypedEncoder[Int] {
    def nullable: Boolean = false

    def sourceDataType: DataType = IntegerType
    def targetDataType: DataType = IntegerType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val longEncoder: TypedEncoder[Long] = new TypedEncoder[Long] {
    def nullable: Boolean = false

    def sourceDataType: DataType = LongType
    def targetDataType: DataType = LongType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val shortEncoder: TypedEncoder[Short] = new TypedEncoder[Short] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ShortType
    def targetDataType: DataType = ShortType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val charEncoder: TypedEncoder[Char] = new TypedEncoder[Char] {
    // tricky because while Char is primitive type, Spark doesn't support it

    implicit val charAsString: Injection[java.lang.Character, String] = new Injection[java.lang.Character, String] {
      def apply(a: java.lang.Character): String = String.valueOf(a)
      def invert(b: String): java.lang.Character = {
        require(b.length == 1)
        b.charAt(0)
      }
    }

    val underlying = usingInjection[java.lang.Character, String]

    def nullable: Boolean = false

    // this line fixes underlying encoder
    def sourceDataType: DataType = FramelessInternals.objectTypeFor[java.lang.Character]
    def targetDataType: DataType = StringType

    def extractorFor(path: Expression): Expression = underlying.extractorFor(path)
    def constructorFor(path: Expression): Expression = underlying.constructorFor(path)
  }

  implicit val byteEncoder: TypedEncoder[Byte] = new TypedEncoder[Byte] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ByteType
    def targetDataType: DataType = ByteType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val floatEncoder: TypedEncoder[Float] = new TypedEncoder[Float] {
    def nullable: Boolean = false

    def sourceDataType: DataType = FloatType
    def targetDataType: DataType = FloatType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val doubleEncoder: TypedEncoder[Double] = new TypedEncoder[Double] {
    def nullable: Boolean = false

    def sourceDataType: DataType = DoubleType
    def targetDataType: DataType = DoubleType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val bigDecimalEncoder: TypedEncoder[BigDecimal] = new TypedEncoder[BigDecimal] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[BigDecimal]
    def targetDataType: DataType = DecimalType.SYSTEM_DEFAULT

    def extractorFor(path: Expression): Expression =
      StaticInvoke(Decimal.getClass, DecimalType.SYSTEM_DEFAULT, "apply", path :: Nil)

    def constructorFor(path: Expression): Expression =
      Invoke(path, "toBigDecimal", sourceDataType)
  }

  implicit val sqlDate: TypedEncoder[SQLDate] = new TypedEncoder[SQLDate] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[SQLDate]
    def targetDataType: DataType = DateType

    def extractorFor(path: Expression): Expression =
      Invoke(path, "days", IntegerType)

    def constructorFor(path: Expression): Expression =
      StaticInvoke(
        staticObject = SQLDate.getClass,
        dataType = sourceDataType,
        functionName = "apply",
        arguments = intEncoder.constructorFor(path) :: Nil,
        propagateNull = true
      )
  }

  implicit val sqlTimestamp: TypedEncoder[SQLTimestamp] = new TypedEncoder[SQLTimestamp] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[SQLTimestamp]
    def targetDataType: DataType = TimestampType

    def extractorFor(path: Expression): Expression =
      Invoke(path, "us", LongType)

    def constructorFor(path: Expression): Expression =
      StaticInvoke(
        staticObject = SQLTimestamp.getClass,
        dataType = sourceDataType,
        functionName = "apply",
        arguments = longEncoder.constructorFor(path) :: Nil,
        propagateNull = true
      )
  }

  implicit def optionEncoder[A](
    implicit
    underlying: TypedEncoder[A]
  ): TypedEncoder[Option[A]] = new TypedEncoder[Option[A]] {
    def nullable: Boolean = true

    def sourceDataType: DataType = FramelessInternals.objectTypeFor[Option[A]](classTag)
    def targetDataType: DataType = underlying.targetDataType

    def extractorFor(path: Expression): Expression = {
      // for primitive types we must manually unbox the value of the object
      underlying.sourceDataType match {
        case IntegerType =>
          Invoke(
            UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Integer], path),
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
            UnwrapOption(ScalaReflection.dataTypeFor[java.lang.Boolean], path),
            "booleanValue",
            BooleanType)

        case other => underlying.extractorFor(UnwrapOption(underlying.sourceDataType, path))
      }
    }

    def constructorFor(path: Expression): Expression =
      WrapOption(underlying.constructorFor(path), underlying.sourceDataType)
  }

  implicit def vectorEncoder[A](
    implicit
    underlying: TypedEncoder[A]
  ): TypedEncoder[Vector[A]] = new TypedEncoder[Vector[A]]() {
    def nullable: Boolean = false

    def sourceDataType: DataType = FramelessInternals.objectTypeFor[Vector[A]](classTag)

    def targetDataType: DataType = DataTypes.createArrayType(underlying.targetDataType)

    def constructorFor(path: Expression): Expression = {
      val arrayData = Invoke(
        MapObjects(
          underlying.constructorFor,
          path,
          underlying.targetDataType
        ),
        "array",
        ScalaReflection.dataTypeFor[Array[AnyRef]]
      )

      StaticInvoke(
        TypedEncoderUtils.getClass,
        sourceDataType,
        "mkVector",
        arrayData :: Nil
      )
    }

    def extractorFor(path: Expression): Expression = {
      // if source `path` is already native for Spark, no need to `map`
      if (ScalaReflection.isNativeType(underlying.sourceDataType)) {
        NewInstance(
          classOf[GenericArrayData],
          path :: Nil,
          dataType = ArrayType(underlying.targetDataType, underlying.nullable)
        )
      } else {
        MapObjects(underlying.extractorFor, path, underlying.sourceDataType)
      }
    }
  }

  /** Encodes things using injection if there is one defined */
  implicit def usingInjection[A: ClassTag, B]
    (implicit inj: Injection[A, B], trb: TypedEncoder[B]): TypedEncoder[A] =
      new TypedEncoder[A] {
        def nullable: Boolean = trb.nullable
        def sourceDataType: DataType = FramelessInternals.objectTypeFor[A](classTag)
        def targetDataType: DataType = trb.targetDataType

        def constructorFor(path: Expression): Expression = {
          val bexpr = trb.constructorFor(path)
          Invoke(Literal.fromObject(inj), "invert", sourceDataType, Seq(bexpr))
        }

        def extractorFor(path: Expression): Expression = {
          val invoke = Invoke(Literal.fromObject(inj), "apply", trb.sourceDataType, Seq(path))
          trb.extractorFor(invoke)
        }
      }

  /** Encodes things as records if there is not Injection defined */
  implicit def usingDerivation[F, G <: HList](
    implicit
    lgen: LabelledGeneric.Aux[F, G],
    recordEncoder: Lazy[RecordEncoderFields[G]],
    classTag: ClassTag[F]
  ): TypedEncoder[F] = new RecordEncoder[F, G]

  /** Encodes things using a Spark SQL's User Defined Type (UDT) if there is one defined in implicit */
  implicit def usingUdt[A >: Null : Udt : ClassTag]: TypedEncoder[A] = {
    val udt = implicitly[Udt[A]]
    val udtInstance = NewInstance(udt.getClass, Nil, dataType = ObjectType(udt.getClass))

    new TypedEncoder[A] {
      def nullable: Boolean = false
      def sourceDataType: DataType = ObjectType(udt.userClass)
      def targetDataType: DataType = udt

      def extractorFor(path: Expression): Expression = Invoke(udtInstance, "serialize", udt, Seq(path))

      def constructorFor(path: Expression): Expression =
        Invoke(udtInstance, "deserialize", ObjectType(udt.userClass), Seq(path))
    }
  }
}
