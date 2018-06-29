package frameless

import shapeless.record.Record
import shapeless.{HList, LabelledGeneric}

/**
  * Type-class to wrap non case class schema into Tuple1. For example: <br>
  * Int                   -> Tuple1[Int] <br>
  * List[Char]            -> Tuple1[Char] <br>
  * case class X(i: Int)  -> No-wrapping <br>
  * @tparam A the type to attempt to wrap
  */
trait SchemaWrapper[A] {
  type Wrapped
  type RecordRep <: HList
}

object SchemaWrapper extends LowPrioritySchemaWrapper{
  type Aux[A, B, C] = SchemaWrapper[A]{ type Wrapped = B; type RecordRep = C }

  def apply[A](implicit schemaWrapper: SchemaWrapper[A]): SchemaWrapper[A] = schemaWrapper

  // First attempt to derive a LabelledGeneric. If that fails it means this is not a case class based schema.
  implicit def deriveSchemaWrapper[A, ARep <: HList]
  (implicit
   i0: LabelledGeneric.Aux[A, ARep]
  ): SchemaWrapper.Aux[A, A, ARep] = new SchemaWrapper[A] {
    type Wrapped = A
    type RecordRep = ARep
  }
}

trait LowPrioritySchemaWrapper {
  implicit def deriveSchemaWrapperTuple1[A]: SchemaWrapper.Aux[A, Tuple1[A], Record.`'_1 -> A`.T] = new SchemaWrapper[A] {
    type Wrapped = Tuple1[A]
    type RecordRep = Record.`'_1 -> A`.T
  }
}