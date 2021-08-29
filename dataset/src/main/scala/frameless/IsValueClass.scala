package frameless

import shapeless._
import shapeless.labelled.FieldType

/** Evidence that `T` is a Value class */
@annotation.implicitNotFound(msg = "${T} is not a Value class")
final class IsValueClass[T] private() {}

object IsValueClass {
  /** Provides an evidence `A` is a Value class */
  implicit def apply[A <: AnyVal, G <: ::[_, HNil], H <: ::[_ <: FieldType[_ <: Symbol, _], HNil]](
    implicit
      i0: LabelledGeneric.Aux[A, G],
    i1: DropUnitValues.Aux[G, H]): IsValueClass[A] = new IsValueClass[A]

}
