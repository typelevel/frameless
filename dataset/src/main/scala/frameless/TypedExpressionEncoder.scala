package frameless

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Literal, CreateNamedStruct}
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {
  def apply[T: TypedEncoder]: ExpressionEncoder[T] = {
    val encoder = TypedEncoder[T]
    val schema = encoder.targetDataType match {
      case x: StructType => x
      case dt => new StructType().add("value", dt, nullable = false)
    }

    val toRowExpressions = encoder.extractor() match {
      case x: CreateNamedStruct => x.flatten
      case other => CreateNamedStruct(Literal("value") :: other :: Nil).flatten
    }

    new ExpressionEncoder[T](
      schema = schema,
      flat = false,
      toRowExpressions = toRowExpressions,
      fromRowExpression = encoder.constructor(),
      clsTag = encoder.classTag
    )
  }
}
