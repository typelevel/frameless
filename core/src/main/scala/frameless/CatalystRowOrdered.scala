package frameless

import scala.annotation.implicitNotFound

/** Types that can be used to sort a dataset by Catalyst. */
@implicitNotFound("Cannot order by columns of type ${A}.")
trait CatalystRowOrdered[A]

object CatalystRowOrdered {
  private[this] val theInstance = new CatalystRowOrdered[Any] {}
  private[this] def of[A]: CatalystRowOrdered[A] = theInstance.asInstanceOf[CatalystRowOrdered[A]]

  /*
  The following are sortable by spark:
  see [[org.apache.spark.sql.catalyst.expressions.RowOrdering.isOrderable]]

    case NullType => true
    case dt: AtomicType => true
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
    case array: ArrayType => isOrderable(array.elementType)
    case udt: UserDefinedType[_] => isOrderable(udt.sqlType)

  Map can't be used in order
  TODO: UDF
  TODO: Struct / Records

   */
  implicit def orderedEvidence[A](implicit catalystOrdered: CatalystOrdered[A]): CatalystRowOrdered[A] = of[A]

  implicit def arrayEv[A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[Array[A]] = of[Array[A]]

  implicit def collectionEv[C[X] <: Seq[X], A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[C[A]] = of[C[A]]

  implicit def optionEv[A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[Option[A]] = of[Option[A]]

}
