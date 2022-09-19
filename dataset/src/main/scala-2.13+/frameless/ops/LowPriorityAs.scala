package frameless.ops

import shapeless.{::, Generic, HList}

trait LowPriorityAs {

  import As.Equiv

  implicit def equivHList[AH, AT <: HList, BH, BT <: HList]
    (implicit
      i0: Equiv[AH, BH],
      i1: Equiv[AT, BT]
    ): Equiv[AH :: AT, BH :: BT] = new Equiv[AH :: AT, BH :: BT]

  implicit def equivGeneric[A, B, R, S]
    (implicit
      i0: Generic.Aux[A, R],
      i1: Generic.Aux[B, S],
      i2: Equiv[R, S]
    ): Equiv[A, B] = new Equiv[A, B]

}
