package frameless

import shapeless.{HList, LabelledGeneric}

trait SchemaWrapper[A] {
  type N
}

object SchemaWrapper extends LowPrioritySchemaWrapper{
  type Aux[A, B] = SchemaWrapper[A]{ type N = B }

  implicit def x[A, ARep <: HList]
  (implicit
   i0: LabelledGeneric.Aux[A, ARep]): SchemaWrapper.Aux[A, A] = new SchemaWrapper[A] {
    type N = A
  }
}

trait LowPrioritySchemaWrapper {
  implicit def g[A]: SchemaWrapper.Aux[A, Tuple1[A]] = new SchemaWrapper[A] {
    type N = Tuple1[A]
  }
}