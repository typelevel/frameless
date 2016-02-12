package frameless

import org.apache.spark.sql.FramelessReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import shapeless.labelled.FieldType
import shapeless._

import scala.reflect.ClassTag

case class RecordEncoderField(
  ordinal: Int,
  name: String,
  encoder: TypedEncoder[_]
)

trait RecordEncoderFields[T <: HList] {
  def value: List[RecordEncoderField]
}

object RecordEncoderFields {
  implicit val deriveNil: RecordEncoderFields[HNil] = new RecordEncoderFields[HNil] {
    def value: List[RecordEncoderField] = Nil
  }

  implicit def deriveRecord[K <: Symbol, H, T <: HList](
    implicit
    key: Witness.Aux[K],
    head: TypedEncoder[H],
    tail: RecordEncoderFields[T]
  ): RecordEncoderFields[FieldType[K, H] :: T] = new RecordEncoderFields[FieldType[K, H] :: T] {
    def value: List[RecordEncoderField] = {
      val fieldName = key.value.name
      val fieldEncoder = RecordEncoderField(0, fieldName, head)

      fieldEncoder :: tail.value.map(x => x.copy(ordinal = x.ordinal + 1))
    }
  }
}

class RecordEncoder[F, G <: HList](
  implicit
  lgen: LabelledGeneric.Aux[F, G],
  fields: Lazy[RecordEncoderFields[G]],
  classTag: ClassTag[F]
) extends TypedEncoder[F] {
  def nullable: Boolean = false

  def sourceDataType: DataType = FramelessReflection.objectTypeFor[F]

  def targetDataType: DataType = {
    val structFields = fields.value.value.map { field =>
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
    val nameExprs = fields.value.value.map { field =>
      Literal(field.name)
    }

    val valueExprs = fields.value.value.map { field =>
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
    val exprs = fields.value.value.map { field =>
      val path = BoundReference(field.ordinal, field.encoder.sourceDataType, field.encoder.nullable)
      field.encoder.constructorFor(path)
    }

    NewInstance(classTag.runtimeClass, exprs, propagateNull = true, sourceDataType)
  }

  def constructorFor(path: Expression): Expression = {
    val exprs = fields.value.value.map { field =>
      val fieldPath = GetStructField(path, field.ordinal, Some(field.name))
      field.encoder.constructorFor(fieldPath)
    }

    NewInstance(classTag.runtimeClass, exprs, propagateNull = true, sourceDataType)
  }
}