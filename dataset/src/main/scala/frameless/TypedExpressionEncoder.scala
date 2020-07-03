package frameless

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, If, Literal}
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {

  /** In Spark, DataFrame has always schema of StructType
    *
    * DataFrames of primitive types become records with a single field called "_1".
    */
  def targetStructType[A](encoder: TypedEncoder[A]): StructType = {
   encoder.catalystRepr match {
      case x: StructType =>
        if (encoder.nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x
      case dt => new StructType().add("_1", dt, nullable = encoder.nullable)
    }
  }

  def apply[T: TypedEncoder]: ExpressionEncoder[T] = {
    val encoder = TypedEncoder[T]
    val targetSchema = targetStructType(encoder)

    val in = BoundReference(0, encoder.jvmRepr, encoder.nullable)

    val (out, toRowExpressions) = encoder.toCatalyst(in) match {
      case If(_, _, x: CreateNamedStruct) =>
        val out = BoundReference(0, encoder.catalystRepr, encoder.nullable)

        (out, x.flatten)
      case other =>
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)

        (out, CreateNamedStruct(Literal("_1") :: other :: Nil).flatten)
    }

    new ExpressionEncoder[T](
      objSerializer = encoder.toCatalyst(in),
      objDeserializer = encoder.fromCatalyst(out),
      clsTag = encoder.classTag
    )
  }
}
