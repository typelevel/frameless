package frameless

import org.apache.spark.sql.FramelessReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import shapeless._

import scala.reflect.ClassTag

case class ProductEncoderField(ordinal: Int, encoder: TypedEncoder[_]) {
  def name: String = s"_${ordinal + 1}"
}

trait ProductEncoderFields[T <: HList] {
  def fields: List[ProductEncoderField]
}

object ProductEncoderFields {
  def fieldName(field: ProductEncoderField): String = s"_${field.ordinal + 1}"

  implicit val deriveNil: ProductEncoderFields[HNil] = new ProductEncoderFields[HNil] {
    def fields: List[ProductEncoderField] = Nil
  }

  implicit def deriveCons[H, T <: HList](
    implicit
    head: TypedEncoder[H],
    tail: ProductEncoderFields[T]
  ): ProductEncoderFields[H :: T] = new ProductEncoderFields[H :: T] {
    def fields: List[ProductEncoderField] = {
      val shiftedOrdinals = tail.fields.map(x => x.copy(ordinal = x.ordinal + 1))
      ProductEncoderField(0, head) :: shiftedOrdinals
    }
  }
}

class ProductEncoder[F, G <: HList](
  implicit
  gen: Generic.Aux[F, G],
  productEncoder: Lazy[ProductEncoderFields[G]],
  classTag: ClassTag[F]
) extends TypedEncoder[F] {
  def nullable: Boolean = false

  def sourceDataType: DataType = FramelessReflection.objectTypeFor[F]

  def targetDataType: DataType = {
    val structFields = productEncoder.value.fields.map { field =>
      StructField(
        name = field.name,
        dataType = field.encoder.targetDataType,
        nullable = field.encoder.nullable,
        metadata = Metadata.empty
      )
    }

    StructType(structFields)
  }

  def extractor(): Expression = {
    extractorFor(BoundReference(0, sourceDataType, nullable = false))
  }

  def extractorFor(path: Expression): Expression = {
    val nameExprs = productEncoder.value.fields.map { field =>
      Literal(field.name)
    }

    val valueExprs = productEncoder.value.fields.map { field =>
      val fieldPath = Invoke(path, field.name, field.encoder.sourceDataType, Nil)
      field.encoder.extractorFor(fieldPath)
    }

    // the way exprs are encoded in CreateNamedStruct
    val exprs = nameExprs.zip(valueExprs).flatMap {
      case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
    }

    CreateNamedStruct(exprs)
  }

  def constructor(): Expression = {
    val exprs = productEncoder.value.fields.map { field =>
      val path = BoundReference(field.ordinal, field.encoder.sourceDataType, field.encoder.nullable)
      field.encoder.constructorFor(path)
    }

    NewInstance(classTag.runtimeClass, exprs, propagateNull = false, sourceDataType)
  }

  def constructorFor(path: Expression): Expression = {
    val exprs = productEncoder.value.fields.map { field =>
      val fieldPath = GetStructField(path, field.ordinal, Some(field.name))
      field.encoder.constructorFor(fieldPath)
    }

    NewInstance(classTag.runtimeClass, exprs, propagateNull = false, sourceDataType)
  }
}
