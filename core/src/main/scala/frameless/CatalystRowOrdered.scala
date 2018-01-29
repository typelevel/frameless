package frameless

import shapeless._

import scala.annotation.implicitNotFound

/** Types that can be used to sort a dataset by Catalyst. */
@implicitNotFound("Cannot order by columns of type ${A}.")
trait CatalystRowOrdered[A]

object CatalystRowOrdered extends CatalystRowOrdered0 {
  /*
  The following are sortable by spark:
  see [[org.apache.spark.sql.catalyst.expressions.RowOrdering.isOrderable]]
    AtomicType
    StructType containing only orderable types
    ArrayType containing only orderable types
    UserDefinedType containing only orderable types

  MapType can't be used in order!
  TODO: UDF
   */

  implicit def orderedEvidence[A](implicit catalystOrdered: CatalystOrdered[A]): CatalystRowOrdered[A] = of[A]

  implicit def arrayEv[A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[Array[A]] = of[Array[A]]

  implicit def collectionEv[C[X] <: Seq[X], A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[C[A]] = of[C[A]]

  implicit def optionEv[A](implicit catalystOrdered: CatalystRowOrdered[A]): CatalystRowOrdered[Option[A]] = of[Option[A]]
}

trait CatalystRowOrdered0 {
  private val theInstance = new CatalystRowOrdered[Any] {}
  protected def of[A]: CatalystRowOrdered[A] = theInstance.asInstanceOf[CatalystRowOrdered[A]]

  implicit def recordEv[A, G <: HList](implicit i0: Generic.Aux[A, G], i1: HasRowOrdered[G]): CatalystRowOrdered[A] = of[A]

  trait HasRowOrdered[T <: HList]
  object HasRowOrdered {
    implicit def deriveOrderHNil[H](implicit catalystRowOrdered: CatalystRowOrdered[H]): HasRowOrdered[H :: HNil] =
      new HasRowOrdered[H :: HNil] {}

    implicit def deriveOrderHCons[H, T <: HList](implicit head: CatalystRowOrdered[H], tail: HasRowOrdered[T]): HasRowOrdered[H :: T] =
      new HasRowOrdered[H :: T] {}
  }
}
