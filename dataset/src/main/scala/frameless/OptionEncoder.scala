package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, UnwrapOption, WrapOption}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType}

case class OptionEncoder[A]()(implicit underlying: TypedEncoder[A]) extends TypedEncoder[Option[A]] {
  def nullable: Boolean = true

  def jvmRepr: DataType = FramelessInternals.objectTypeFor[Option[A]](classTag)
  def catalystRepr: DataType = underlying.catalystRepr

  def toCatalyst(path: Expression): Expression = {
    // for primitive types we must manually unbox the value of the object
    underlying.jvmRepr match {
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

      case _ => underlying.toCatalyst(UnwrapOption(underlying.jvmRepr, path))
    }
  }

  def fromCatalyst(path: Expression): Expression =
    WrapOption(underlying.fromCatalyst(path), underlying.jvmRepr)
}
