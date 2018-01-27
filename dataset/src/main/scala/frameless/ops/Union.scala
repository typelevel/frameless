package frameless
package ops

import org.apache.spark.sql.Column
import shapeless.ops.hlist.Align
import shapeless.{HList, LabelledGeneric}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot prove that ${T} can be unioned with ${U}. Perhaps not all member names and types of ${U} are the same in ${T}?")
final case class Union[T: TypedEncoder, U: TypedEncoder](apply: (TypedDataset[T], TypedDataset[U]) => TypedDataset[T])

object Union {
  /**
    * Proofs that there is a type-safe projection from a type T to another type U. It requires that:
    * (a) both T and U are Products for which a LabelledGeneric can be derived (e.g., case classes),
    * (b) all members of T and U have the same type and name but they might be in different order.
    *
    * @param g1 the LabelledGeneric derived for T
    * @param g2 the LabelledGeneric derived for U
    * @param A the Align derived for RT and RU
    * @tparam T the original type T of first dataset
    * @tparam U the original type U of second dataset
    * @tparam RT the labelled generic representation of the type T
    * @tparam RU the labelled generic representation of the type U
    * @return an union if fields are aligned
    */
  implicit def deriveProduct[T: TypedEncoder, U: TypedEncoder, RT <: HList, RU <: HList]
    (implicit
      g1: LabelledGeneric.Aux[T, RT],
      g2: LabelledGeneric.Aux[U, RU],
      A: Align[RT, RU]
    ): Union[T, U] = Union[T, U] { (self, other) =>
      val columns = self.dataset.columns.map(new Column(_)).toSeq
      val alignedDataset = TypedDataset.createUnsafe[T](other.dataset.select(columns: _*))
      TypedDataset.create(self.dataset.union(alignedDataset.dataset))
    }
}