package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import shapeless._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

abstract class TypedEncoder[T](implicit val classTag: ClassTag[T]) {
  def nullable: Boolean

  def sourceDataType: DataType
  def targetDataType: DataType

  def constructor(): Expression
  def constructorFor(path: Expression): Expression

  def extractor(): Expression
  def extractorFor(path: Expression): Expression
}

abstract class PrimitiveTypedEncoder[T: ClassTag] extends TypedEncoder[T] {
  def constructor(): Expression = constructorFor(BoundReference(0, targetDataType, nullable))

  def extractor(): Expression = extractorFor(BoundReference(0, sourceDataType, nullable))
}

object TypedEncoder extends LowPriorityTypedEncoder {
  def apply[T: TypedEncoder]: TypedEncoder[T] = implicitly[TypedEncoder[T]]

  implicit val unitEncoder: TypedEncoder[Unit] = new PrimitiveTypedEncoder[Unit] {
    def nullable: Boolean = true

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[Unit]
    def targetDataType: DataType = NullType

    def constructorFor(path: Expression): Expression = Literal.create((), sourceDataType)
    def extractorFor(path: Expression): Expression = Literal.create(null, targetDataType)
  }

  implicit val stringEncoder: TypedEncoder[String] = new PrimitiveTypedEncoder[String] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[String]
    def targetDataType: DataType = StringType

    def extractorFor(path: Expression): Expression = {
      StaticInvoke(classOf[UTF8String], targetDataType, "fromString", path :: Nil)
    }

    def constructorFor(path: Expression): Expression = {
      Invoke(path, "toString", sourceDataType)
    }
  }

  implicit val booleanEncoder: TypedEncoder[Boolean] = new PrimitiveTypedEncoder[Boolean] {
    def nullable: Boolean = false

    def sourceDataType: DataType = BooleanType
    def targetDataType: DataType = BooleanType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val intEncoder: TypedEncoder[Int] = new PrimitiveTypedEncoder[Int] {
    def nullable: Boolean = false

    def sourceDataType: DataType = IntegerType
    def targetDataType: DataType = IntegerType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val longEncoder: TypedEncoder[Long] = new PrimitiveTypedEncoder[Long] {
    def nullable: Boolean = false

    def sourceDataType: DataType = LongType
    def targetDataType: DataType = LongType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val shortEncoder: TypedEncoder[Short] = new PrimitiveTypedEncoder[Short] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ShortType
    def targetDataType: DataType = ShortType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val byteEncoder: TypedEncoder[Byte] = new PrimitiveTypedEncoder[Byte] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ByteType
    def targetDataType: DataType = ByteType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val floatEncoder: TypedEncoder[Float] = new PrimitiveTypedEncoder[Float] {
    def nullable: Boolean = false

    def sourceDataType: DataType = FloatType
    def targetDataType: DataType = FloatType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val doubleEncoder: TypedEncoder[Double] = new PrimitiveTypedEncoder[Double] {
    def nullable: Boolean = false

    def sourceDataType: DataType = DoubleType
    def targetDataType: DataType = DoubleType

    def extractorFor(path: Expression): Expression = path
    def constructorFor(path: Expression): Expression = path
  }

  implicit val bigDecimalEncoder: TypedEncoder[BigDecimal] = new PrimitiveTypedEncoder[BigDecimal] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[BigDecimal]
    def targetDataType: DataType = DecimalType.SYSTEM_DEFAULT

    def extractorFor(path: Expression): Expression = {
      StaticInvoke(Decimal.getClass, DecimalType.SYSTEM_DEFAULT, "apply", path :: Nil)
    }

    def constructorFor(path: Expression): Expression = {
      Invoke(path, "toBigDecimal", sourceDataType, arguments = Nil)
    }
  }

  implicit val sqlDate: TypedEncoder[SQLDate] = new PrimitiveTypedEncoder[SQLDate] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[SQLDate]
    def targetDataType: DataType = DateType

    def extractorFor(path: Expression): Expression = {
      Invoke(path, "days", IntegerType, arguments = Nil)
    }

    def constructorFor(path: Expression): Expression = {
      StaticInvoke(
        staticObject = SQLDate.getClass,
        dataType = sourceDataType,
        functionName = "apply",
        arguments = intEncoder.constructorFor(path) :: Nil,
        propagateNull = true
      )
    }
  }

  implicit val sqlTimestamp: TypedEncoder[SQLTimestamp] = new PrimitiveTypedEncoder[SQLTimestamp] {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[SQLTimestamp]
    def targetDataType: DataType = TimestampType

    def extractorFor(path: Expression): Expression = {
      Invoke(path, "us", LongType, arguments = Nil)
    }

    def constructorFor(path: Expression): Expression = {
      StaticInvoke(
        staticObject = SQLTimestamp.getClass,
        dataType = sourceDataType,
        functionName = "apply",
        arguments = longEncoder.constructorFor(path) :: Nil,
        propagateNull = true
      )
    }
  }

  implicit def optionEncoder[T](
    implicit
    underlying: TypedEncoder[T],
    typeTag: TypeTag[Option[T]]
  ): TypedEncoder[Option[T]] = new TypedEncoder[Option[T]] {
    def nullable: Boolean = true

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[Option[T]]
    def targetDataType: DataType = underlying.targetDataType

    def constructor(): Expression = {
      WrapOption(underlying.constructor())
    }

    def extractor(): Expression = extractorFor(BoundReference(0, sourceDataType, nullable))

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

    def constructorFor(path: Expression): Expression = {
      WrapOption(underlying.constructorFor(path))
    }
  }

  implicit def vectorEncoder[A](
    implicit
    underlying: TypedEncoder[A],
    typeTag: TypeTag[A]
  ): TypedEncoder[Vector[A]] = new PrimitiveTypedEncoder[Vector[A]]() {
    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[Vector[A]]

    def targetDataType: DataType = DataTypes.createArrayType(underlying.targetDataType)

    def constructorFor(path: Expression): Expression = {
      val arrayData = Invoke(
        MapObjects(
          underlying.constructorFor,
          path,
          underlying.targetDataType
        ),
        "array",
        ScalaReflection.dataTypeFor[Array[Any]]
      )

      StaticInvoke(
        TypedEncoderUtils.getClass,
        ScalaReflection.dataTypeFor[Vector[_]],
        "mkVector",
        arrayData :: Nil
      )
    }

    def extractorFor(path: Expression): Expression = {
      if (ScalaReflection.isNativeType(underlying.targetDataType)) {
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

  /** Encodes things as records if */
  implicit def deriveRecordEncoder[F, G <: HList](
    implicit
    lgen: LabelledGeneric.Aux[F, G],
    recordEncoder: Lazy[RecordEncoderFields[G]],
    classTag: ClassTag[F]
  ): TypedEncoder[F] = new RecordEncoder[F, G]
}

trait LowPriorityTypedEncoder {
  /** Encodes things as products if they aren't records. */
  implicit def deriveProductEncoder[F, G <: HList](
    implicit
    gen: Generic.Aux[F, G],
    productEncoder: Lazy[ProductEncoderFields[G]],
    classTag: ClassTag[F]
  ): TypedEncoder[F] = new ProductEncoder[F, G]
}
