package frameless
package ops

/** Evidence for correctness of `TypedDataset[T].as[U]` */
class As[T, U] private (implicit val encoder: TypedEncoder[U])

object As extends LowPriorityAs {

  final class Equiv[A, B] private[ops] ()

  implicit def equivIdentity[A] = new Equiv[A, A]

  implicit def deriveAs[A, B]
    (implicit
      i0: TypedEncoder[B],
      i1: Equiv[A, B]
    ): As[A, B] = new As[A, B]

}
