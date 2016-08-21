package frameless

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Literal}
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {
  def apply[T: TypedEncoder]: ExpressionEncoder[T] = {
    val encoder = TypedEncoder[T]
    val schema = encoder.targetDataType match {
      case x: StructType =>
        if (encoder.nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x
      case dt => new StructType().add("value", dt, nullable = encoder.nullable)
    }

    val in = BoundReference(0, encoder.sourceDataType, encoder.nullable)

    val (out, toRowExpressions) = encoder.extractorFor(in) match {
      case x: CreateNamedStruct =>
        val out = BoundReference(0, encoder.targetDataType, encoder.nullable)

        (out, x.flatten)
      case other =>
        val out = GetColumnByOrdinal(0, encoder.targetDataType)

        (out, CreateNamedStruct(Literal("value") :: other :: Nil).flatten)
    }

    new ExpressionEncoder[T](
      schema = schema,
      flat = false,
      serializer = toRowExpressions,
      deserializer = encoder.constructorFor(out),
      clsTag = encoder.classTag
    )
  }
}
