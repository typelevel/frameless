package frameless

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, If, IsNull, Literal}
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {

  /** In Spark, DataFrame has always schema of StructType
    *
    * DataFrames of primitive types become records with a single field called "value" set in ExpressionEncoder.
    */
  def targetStructType[A](encoder: TypedEncoder[A]): StructType = {
   encoder.catalystRepr match {
      case x: StructType =>
        if (encoder.nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x
      case dt => new StructType().add("value", dt, nullable = encoder.nullable)
    }
  }

  def apply[T: TypedEncoder]: Encoder[T] = {
    val encoder = TypedEncoder[T]
    val in = BoundReference(0, encoder.jvmRepr, encoder.nullable)

    val (objectDeserializer, serializer) = encoder.toCatalyst(in) match {
      case it @ If(a, b, x: CreateNamedStruct) =>
       //
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)
          // 48
          BoundReference(0, encoder.catalystRepr, encoder.nullable)

        // will be newInstance
// 48
        (If(IsNull(GetColumnByOrdinal(0, encoder.catalystRepr)), Literal.create(null, encoder.catalystRepr), encoder.fromCatalyst(out)), it)
        // 48, so no difference
        (encoder.fromCatalyst(out), it)
      case other =>
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)

       ( encoder.fromCatalyst(out), other)
    }

    new ExpressionEncoder[T](
      objSerializer = serializer,
      objDeserializer = objectDeserializer,
      clsTag = encoder.classTag
    )
  }
}

