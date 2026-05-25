package frameless
package ops

import shapeless.{ ::, Generic, HList, Lazy }

/** Evidence for correctness of `TypedDataset[T].as[U]` */
class As[T, U] private (
    implicit
    val encoder: TypedEncoder[U])

object As extends LowPriorityAs {

  final class Equiv[A, B] private[ops] ()

  implicit def equivIdentity[A] = new Equiv[A, A]

  implicit def deriveAs[A, B](
      implicit
      i0: TypedEncoder[B],
      i1: Equiv[A, B]
    ): As[A, B] = new As[A, B]

}

trait LowPriorityAs {

  import As.Equiv

  implicit def equivHList[AH, AT <: HList, BH, BT <: HList](
      implicit
      i0: Lazy[Equiv[AH, BH]],
      i1: Equiv[AT, BT]
    ): Equiv[AH :: AT, BH :: BT] = new Equiv[AH :: AT, BH :: BT]

  implicit def equivGeneric[A, B, R, S](
      implicit
      i0: Generic.Aux[A, R],
      i1: Generic.Aux[B, S],
      i2: Lazy[Equiv[R, S]]
    ): Equiv[A, B] = new Equiv[A, B]

}
