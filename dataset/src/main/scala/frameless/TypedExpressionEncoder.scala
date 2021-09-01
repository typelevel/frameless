package frameless

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, If}
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {

  /**
   * In Spark, DataFrame has always schema of StructType
   *
   * DataFrames of primitive types become records with a single field called "value" set in
   * ExpressionEncoder.
   */
  def targetStructType[A](encoder: TypedEncoder[A]): StructType = {
    encoder.catalystRepr match {
      case x: StructType =>
        if (encoder.nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x

      case dt => new StructType().add("value", dt, nullable = encoder.nullable)
    }

  def apply[T](implicit encoder: TypedEncoder[T]): Encoder[T] = {
    val in = BoundReference(0, encoder.jvmRepr, encoder.nullable)

    val (out, serializer) = encoder.toCatalyst(in) match {
      case it @ If(_, _, _: CreateNamedStruct) => {
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)

        out -> it
      }

      case other => {
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)

        out -> other
      }
    }

    new ExpressionEncoder[T](
      objSerializer = serializer,
      objDeserializer = encoder.fromCatalyst(out),
      clsTag = encoder.classTag
    )
  }
}
