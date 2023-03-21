package frameless

import scala.reflect.ClassTag

import org.apache.spark.sql.types._

import org.apache.spark.sql.FramelessInternals

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}

import shapeless._
import shapeless.ops.hlist.IsHCons

private[frameless] trait TypedEncoderCompat {
  /** Encodes things using injection if there is one defined */
  implicit def usingInjection[A: ClassTag, B]
    (implicit inj: Injection[A, B], trb: TypedEncoder[B]): TypedEncoder[A] =
      new TypedEncoder[A] {
        def nullable: Boolean = trb.nullable
        def jvmRepr: DataType = FramelessInternals.objectTypeFor[A](classTag)
        def catalystRepr: DataType = trb.catalystRepr

        def fromCatalyst(path: Expression): Expression = {
          val bexpr = trb.fromCatalyst(path)
          Invoke(Literal.fromObject(inj), "invert", jvmRepr, Seq(bexpr))
        }

        def toCatalyst(path: Expression): Expression =
          trb.toCatalyst(Invoke(
            Literal.fromObject(inj), "apply", trb.jvmRepr, Seq(path)))
      }

  /** Encodes things as records if there is no Injection defined */
  implicit def usingDerivation[F, G <: HList, H <: HList]
    (implicit
      i0: LabelledGeneric.Aux[F, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons[H],
      i3: Lazy[RecordEncoderFields[H]],
      i4: Lazy[NewInstanceExprs[G]],
      i5: ClassTag[F]
    ): TypedEncoder[F] = new RecordEncoder[F, G, H]

  implicit def arrayEncoder[T: ClassTag](
    implicit i0: Lazy[RecordFieldEncoder[T]]): TypedEncoder[Array[T]] =
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
          case IntegerType | LongType | DoubleType | FloatType |
              ShortType | BooleanType =>
            StaticInvoke(
              classOf[UnsafeArrayData],
              catalystRepr, "fromPrimitiveArray", path :: Nil)

          case ByteType => path

          case _ => MapObjects(
            enc.toCatalyst, path, enc.jvmRepr, encodeT.nullable)
        }
      }

      def fromCatalyst(path: Expression): Expression =
        encodeT.jvmRepr match {
          case IntegerType => Invoke(path, "toIntArray", jvmRepr)
          case LongType => Invoke(path, "toLongArray", jvmRepr)
          case DoubleType => Invoke(path, "toDoubleArray", jvmRepr)
          case FloatType => Invoke(path, "toFloatArray", jvmRepr)
          case ShortType => Invoke(path, "toShortArray", jvmRepr)
          case BooleanType => Invoke(path, "toBooleanArray", jvmRepr)

          case ByteType => path

          case _ =>
            Invoke(MapObjects(
              i0.value.fromCatalyst, path,
              encodeT.catalystRepr, encodeT.nullable), "array", jvmRepr)
        }

      override def toString: String = s"arrayEncoder($jvmRepr)"
    }

  implicit def collectionEncoder[C[X] <: Seq[X], T]
    (implicit
      i0: Lazy[RecordFieldEncoder[T]],
      i1: ClassTag[C[T]]): TypedEncoder[C[T]] = new TypedEncoder[C[T]] {
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
   * @tparam A the key type
   * @tparam B the value type
   * @param i0 the keys encoder
   * @param i1 the values encoder
   */
  implicit def mapEncoder[A: NotCatalystNullable, B]
    (implicit
      i0: Lazy[RecordFieldEncoder[A]],
      i1: Lazy[RecordFieldEncoder[B]],
    ): TypedEncoder[Map[A, B]] = new TypedEncoder[Map[A, B]] {
      def nullable: Boolean = false

      def jvmRepr: DataType = FramelessInternals.objectTypeFor[Map[A, B]]

      private lazy val encodeA = i0.value.encoder
      private lazy val encodeB = i1.value.encoder

      lazy val catalystRepr: DataType = MapType(
        encodeA.catalystRepr, encodeB.catalystRepr, encodeB.nullable)

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
          keyData :: valueData :: Nil)
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
          encodeB.nullable)
      }

      override def toString = s"mapEncoder($jvmRepr)"
    }
}
