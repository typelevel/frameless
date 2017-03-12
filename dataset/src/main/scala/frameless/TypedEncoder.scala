package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import shapeless._

import scala.collection.Map
import scala.reflect.ClassTag

abstract class TypedEncoder[T](implicit val classTag: ClassTag[T]) extends Serializable {
  def nullable: Boolean

  def sourceDataType: DataType
  def targetDataType: DataType

  def constructorFor(path: Expression): Expression
  def extractorFor(path: Expression): Expression
}

// Waiting on scala 2.12
// @annotation.implicitAmbiguous(msg =
// """TypedEncoder[${T}] can be obtained both from automatic type class derivation and using the implicit Injection[${T}, ?] in scope. To desambigious this resolution you need to either:
//   - Remove the implicit Injection[${T}, ?] from scope
//   - import TypedEncoder.usingInjection
//   - import TypedEncoder.usingDerivation
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

  implicit def vectorEncoder[A : ClassTag](
    implicit
    underlying: TypedEncoder[A]
  ): TypedEncoder[Vector[A]] = new TypedEncoder[Vector[A]]() {
    val nullable: Boolean = false

    val sourceDataType: DataType = FramelessInternals.objectTypeFor[Vector[A]](classTag)

    val targetDataType: DataType = DataTypes.createArrayType(underlying.targetDataType)

    def constructorFor(path: Expression): Expression = {
      val arrayData = Option(underlying.sourceDataType)
        .filter(ScalaReflection.isNativeType)
        .filter(_ == underlying.targetDataType)
        .collect {
          case BooleanType => "toBooleanArray" -> ScalaReflection.dataTypeFor[Array[Boolean]]
          case ByteType => "toByteArray" -> ScalaReflection.dataTypeFor[Array[Byte]]
          case ShortType => "toShortArray" -> ScalaReflection.dataTypeFor[Array[Short]]
          case IntegerType => "toIntArray" -> ScalaReflection.dataTypeFor[Array[Int]]
          case LongType => "toLongArray" -> ScalaReflection.dataTypeFor[Array[Long]]
          case FloatType => "toFloatArray" -> ScalaReflection.dataTypeFor[Array[Float]]
          case DoubleType => "toDoubleArray" -> ScalaReflection.dataTypeFor[Array[Double]]
        }.map {
          case (method, typ) => Invoke(path, method, typ)
        }.getOrElse {
          Invoke(
            MapObjects(
              underlying.constructorFor,
              path,
              underlying.targetDataType
            ),
            "array",
            FramelessInternals.objectTypeFor[Array[A]]
          )
        }

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

  implicit def arrayEncoder[A : ClassTag](
    implicit
    underlying: TypedEncoder[A]
  ): TypedEncoder[Array[A]] = new TypedEncoder[Array[A]]() {
    val nullable: Boolean = false

    val sourceDataType: DataType = FramelessInternals.objectTypeFor[Array[A]](classTag)

    val targetDataType: DataType = DataTypes.createArrayType(underlying.targetDataType)

    def constructorFor(path: Expression): Expression = {
      Option(underlying.sourceDataType)
        .filter(ScalaReflection.isNativeType)
        .filter(_ == underlying.targetDataType)
        .collect {
          case BooleanType => "toBooleanArray" -> ScalaReflection.dataTypeFor[Array[Boolean]]
          case ByteType => "toByteArray" -> ScalaReflection.dataTypeFor[Array[Byte]]
          case ShortType => "toShortArray" -> ScalaReflection.dataTypeFor[Array[Short]]
          case IntegerType => "toIntArray" -> ScalaReflection.dataTypeFor[Array[Int]]
          case LongType => "toLongArray" -> ScalaReflection.dataTypeFor[Array[Long]]
          case FloatType => "toFloatArray" -> ScalaReflection.dataTypeFor[Array[Float]]
          case DoubleType => "toDoubleArray" -> ScalaReflection.dataTypeFor[Array[Double]]
        }.map {
        case (method, typ) => Invoke(path, method, typ)
      }.getOrElse {
        Invoke(
          MapObjects(
            underlying.constructorFor,
            path,
            underlying.targetDataType
          ),
          "array",
          sourceDataType
        )
      }
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

  implicit def mapEncoder[A : ClassTag, B : ClassTag](
    implicit
    encodeA: TypedEncoder[A],
    encodeB: TypedEncoder[B]
  ): TypedEncoder[scala.collection.immutable.Map[A, B]] = new TypedEncoder[scala.collection.immutable.Map[A, B]] {
    val nullable: Boolean = false
    val sourceDataType = FramelessInternals.objectTypeFor[Map[A, B]]
    val targetDataType = MapType(encodeA.targetDataType, encodeB.targetDataType, encodeB.nullable)

    implicit val classTagArrayA = implicitly[ClassTag[A]].wrap
    implicit val classTagArrayB = implicitly[ClassTag[B]].wrap

    private val arrayA = arrayEncoder[A]
    private val arrayB = arrayEncoder[B]
    private val vectorA = vectorEncoder[A]
    private val vectorB = vectorEncoder[B]


    private def wrap(arrayData: Expression) = {
      StaticInvoke(
        scala.collection.mutable.WrappedArray.getClass,
        FramelessInternals.objectTypeFor[Seq[_]],
        "make",
        arrayData :: Nil)
    }

    def constructorFor(path: Expression): Expression = {
      val keyArrayType = ArrayType(encodeA.targetDataType, false)
      val keyData = wrap(arrayA.constructorFor(Invoke(path, "keyArray", keyArrayType)))

      val valueArrayType = ArrayType(encodeB.targetDataType, encodeB.nullable)
      val valueData = wrap(arrayB.constructorFor(Invoke(path, "valueArray", valueArrayType)))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        sourceDataType,
        "toScalaMap",
        keyData :: valueData :: Nil)
    }

    def extractorFor(path: Expression): Expression = {
      val keys =
        Invoke(
          Invoke(path, "keysIterator", FramelessInternals.objectTypeFor[scala.collection.Iterator[A]]),
          "toVector",
          vectorA.sourceDataType)
      val convertedKeys = arrayA.extractorFor(keys)

      val values =
        Invoke(
          Invoke(path, "valuesIterator", FramelessInternals.objectTypeFor[scala.collection.Iterator[B]]),
          "toVector",
          vectorB.sourceDataType)
      val convertedValues = arrayB.extractorFor(values)

      NewInstance(
        classOf[ArrayBasedMapData],
        convertedKeys :: convertedValues :: Nil,
        dataType = targetDataType)
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
}