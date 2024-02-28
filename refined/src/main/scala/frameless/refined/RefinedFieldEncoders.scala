package frameless.refined

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.sparkutils.shim.expressions.{UnwrapOption2 => UnwrapOption, WrapOption2 => WrapOption}
import org.apache.spark.sql.shim.{Invoke5 => Invoke, NewInstance4 => NewInstance}

import eu.timepit.refined.api.RefType

import frameless.{ TypedEncoder, RecordFieldEncoder }

private[refined] trait RefinedFieldEncoders {
  /**
   * @tparam T the refined type (e.g. `String`)
   */
  implicit def optionRefined[F[_, _], T, R](
    implicit
      i0: RefType[F],
      i1: TypedEncoder[T],
      i2: ClassTag[F[T, R]],
  ): RecordFieldEncoder[Option[F[T, R]]] =
    RecordFieldEncoder[Option[F[T, R]]](new TypedEncoder[Option[F[T, R]]] {
      def nullable = true

      // `Refined` is a Value class: https://github.com/fthomas/refined/blob/master/modules/core/shared/src/main/scala-3.0-/eu/timepit/refined/api/Refined.scala#L8
      def jvmRepr = ObjectType(classOf[Option[F[T, R]]])

      def catalystRepr: DataType = i1.catalystRepr

      val innerJvmRepr = ObjectType(i2.runtimeClass)

      def fromCatalyst(path: Expression): Expression = {
        val javaValue = i1.fromCatalyst(path)
        val value = NewInstance(i2.runtimeClass, Seq(javaValue), innerJvmRepr)

        WrapOption(value, innerJvmRepr)
      }

      @inline def toCatalyst(path: Expression): Expression = {
        val value = UnwrapOption(innerJvmRepr, path)

        val javaValue = Invoke(value, "value", i1.jvmRepr, Nil)

        i1.toCatalyst(javaValue)
      }

      override def toString = s"optionRefined[${i2.runtimeClass.getName}]"
    })

  /**
   * @tparam T the refined type (e.g. `String`)
   */
  implicit def refined[F[_, _], T, R](
    implicit
      i0: RefType[F],
      i1: TypedEncoder[T],
      i2: ClassTag[F[T, R]],
  ): RecordFieldEncoder[F[T, R]] =
    RecordFieldEncoder[F[T, R]](new TypedEncoder[F[T, R]] {
      def nullable = i1.nullable

      // `Refined` is a Value class: https://github.com/fthomas/refined/blob/master/modules/core/shared/src/main/scala-3.0-/eu/timepit/refined/api/Refined.scala#L8
      def jvmRepr = i1.jvmRepr

      def catalystRepr: DataType = i1.catalystRepr

      def fromCatalyst(path: Expression): Expression =
        i1.fromCatalyst(path)

      @inline def toCatalyst(path: Expression): Expression =
        i1.toCatalyst(path)

      override def toString = s"refined[${i2.runtimeClass.getName}]"
    })
}

