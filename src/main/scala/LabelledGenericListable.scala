package typedframe

import shapeless._
import shapeless.ops.hlist.ToList
import shapeless.ops.record.Keys

trait LabelledGenericListable[S <: Product] {
  def list: Seq[String]
}

object LabelledGenericListable {
  def apply[S <: Product](implicit t: LabelledGenericListable[S]): LabelledGenericListable[S] = t
  
  implicit def labelledGenericListable[S <: Product, B <: HList, Y <: HList]
    (implicit
      b: LabelledGeneric.Aux[S, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): LabelledGenericListable[S] =
      new LabelledGenericListable[S] {
        val list = y().toList.map(_.name)
      }
}
