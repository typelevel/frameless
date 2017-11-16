package frameless
package ops

import shapeless.{Generic, HList}

class As[T, U](implicit val encoder: TypedEncoder[U])

object As {
  implicit def deriveProduct[T, U, S <: HList]
    (implicit
      i0: TypedEncoder[U],
      i1: Generic.Aux[T, S],
      i2: Generic.Aux[U, S]
    ): As[T, U] = new As[T, U]
}
