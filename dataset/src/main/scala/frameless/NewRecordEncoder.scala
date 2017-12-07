package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.types._
import scala.annotation.{ tailrec, varargs }
import scala.reflect.ClassTag
import shapeless.labelled.FieldType
import shapeless._

trait RecordCatalystCodec[T <: HList] extends Serializable {
  def fields: List[RecordCatalystCodec.Field]
}

object RecordCatalystCodec extends RecordCatalystCodecInstances {
  case class Field(name: Option[String], encoder: TypedEncoder[_], ordinal: Int) {
    def nameOrOrdinal: String = name.getOrElse(s"_${ordinal + 1}")
  }

  def catalystRepr[F <: HList](codec: RecordCatalystCodec[F]): DataType =
    StructType(
      codec.fields map { field =>
        StructField(
          name = field.nameOrOrdinal,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }
    )

  def toCatalyst[F <: HList](codec: RecordCatalystCodec[F], path: Expression,
    fieldAccessor: Field => Expression): Expression = {
    val exprs = codec.fields flatMap { field =>
      Literal.create(field.nameOrOrdinal) ::
      field.encoder.toCatalyst(fieldAccessor(field)) ::
      Nil
    }

    CreateNamedStruct(exprs)
  }

  def fromCatalyst[F <: HList](codec: RecordCatalystCodec[F], path: Expression): List[Expression] =
    codec.fields map { field =>
      val fieldPath = path match {
        case BoundReference(_, _, _) =>
          GetColumnByOrdinal(field.ordinal, field.encoder.catalystRepr)
        case _ =>
          GetStructField(path, field.ordinal, Some(field.nameOrOrdinal))
      }

      field.encoder.fromCatalyst(fieldPath)
    }
}

trait RecordCatalystCodecInstances extends RecordCatalystCodecInstances0 {
  import RecordCatalystCodec.Field

  implicit val labelledHNilCodec: RecordCatalystCodec[HNil] = new RecordCatalystCodec[HNil] {
    def fields: List[Field] = Nil
  }

  implicit def labelledHConsCodec[K <: Symbol, V, T <: HList](
    implicit
      i0: Witness.Aux[K],
      i1: TypedEncoder[V],
      i2: RecordCatalystCodec[T]
  ): RecordCatalystCodec[FieldType[K, V] :: T] = new RecordCatalystCodec[FieldType[K, V] :: T] {
    def fields: List[Field] =
      Field(Some(i0.value.name), i1, 0) ::
        i2.fields.map(x => x.copy(ordinal = x.ordinal + 1))
  }
}

trait RecordCatalystCodecInstances0 {
  import RecordCatalystCodec.Field

  implicit def plainHConsCodec[V, T <: HList](
    implicit
      i0: TypedEncoder[V],
      i1: RecordCatalystCodec[T]
  ): RecordCatalystCodec[V :: T] = new RecordCatalystCodec[V :: T] {
    def fields: List[Field] =
      Field(None, i0, 0) ::
        i1.fields.map(x => x.copy(ordinal = x.ordinal + 1))

  }
}

object HListUnsafeUtils {
  def unsafeToList(hlist: HList): List[Any] = {
    @tailrec
    def loop(acc: List[Any], rest: HList): List[Any] =
      rest match {
        case h :: t => loop(h :: acc, t)
        case HNil   => acc
      }

    loop(Nil, hlist).reverse
  }

  @varargs
  def unsafeToHCons(xs: Any*): shapeless.::[_, _] = {
    @tailrec
    def loop(acc: HList, rest: Seq[Any]): HList =
      rest match {
        case h +: t => loop(h :: acc, t)
        case Nil    => acc
      }

    HList.unsafeReverse(loop(HNil, xs)).asInstanceOf[shapeless.::[_, _]]
  }
}

class NewRecordEncoder[G <: HList : ClassTag](
  implicit codec: Lazy[RecordCatalystCodec[G]]
) extends TypedEncoder[G] {

  def nullable: Boolean = false

  def jvmRepr: DataType = FramelessInternals.objectTypeFor[G]

  def catalystRepr: DataType = RecordCatalystCodec.catalystRepr(codec.value)

  def toCatalyst(path: Expression): Expression = {
    val anyList = StaticInvoke(
      staticObject = HListUnsafeUtils.getClass,
      dataType = FramelessInternals.objectTypeFor[List[Any]],
      functionName = "unsafeToList",
      arguments = path :: Nil
    )

    RecordCatalystCodec.toCatalyst(
      codec.value,
      path,
      f => Invoke(anyList, "apply", f.encoder.jvmRepr, Literal.create(f.ordinal) :: Nil)
    )
  }

  def fromCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = HListUnsafeUtils.getClass,
      dataType = jvmRepr,
      functionName = "unsafeToHCons",
      arguments = RecordCatalystCodec.fromCatalyst(codec.value, path)
    )
}

class OldRecordEncoder[F: ClassTag, G <: HList](
  implicit
    lgen: LabelledGeneric.Aux[F, G],
    codec: Lazy[RecordCatalystCodec[G]]
) extends TypedEncoder[F] {
  def nullable: Boolean = false

  def jvmRepr: DataType = FramelessInternals.objectTypeFor[F]

  def catalystRepr: DataType = RecordCatalystCodec.catalystRepr(codec.value)

  def toCatalyst(path: Expression) = RecordCatalystCodec.toCatalyst(
    codec.value,
    path,
    f => Invoke(path, f.nameOrOrdinal, f.encoder.jvmRepr, Nil)
  )


  def fromCatalyst(path: Expression): Expression =
    NewInstance(
      classTag.runtimeClass,
      RecordCatalystCodec.fromCatalyst(codec.value, path),
      jvmRepr,
      propagateNull = true
    )
}
