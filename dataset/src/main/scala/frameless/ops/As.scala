package frameless.ops

import frameless.TypedEncoder
import shapeless.{Generic, HList}

class As[T, U](implicit val encoder: TypedEncoder[U])

object As {
  implicit def deriveProduct[T, U, S <: HList](
    implicit
    e: TypedEncoder[U],
    t: Generic.Aux[T, S],
    u: Generic.Aux[U, S]
  ): As[T, U] = new As[T, U]

  implicit def deriveTuple1[U](
    implicit
    e: TypedEncoder[U]
  ): As[Tuple1[U], U] = new As[Tuple1[U], U]
}
