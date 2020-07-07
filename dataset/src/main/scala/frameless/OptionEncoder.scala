package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, UnwrapOption, WrapOption}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ObjectType, ShortType}

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

import org.apache.spark.sql.catalyst.expressions.codegen.Block._

/**
 * Converts the result of evaluating `child` into an option, checking both the isNull bit and
 * (in the case of reference types) equality with null.
 *
 * @param child The expression to evaluate and wrap.
 * @param optType The type of this option.
 */
case class FramelessWrapOption(child: Expression, optType: DataType)
  extends UnaryExpression with NonSQLExpression {

  override def dataType: DataType = ObjectType(classOf[Option[_]])

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = Option(child.eval(input))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)

    val code = inputObject.code + code"""
      scala.Option ${ev.value} =
        ${inputObject.isNull} ?
        scala.Option$$.MODULE$$.apply(null) : new scala.Some(${inputObject.value});
    """
    ev.copy(code = code, isNull = FalseLiteral)
  }
}