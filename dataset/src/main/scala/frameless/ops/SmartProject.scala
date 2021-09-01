package frameless
package ops

import scala.annotation.implicitNotFound

import shapeless.{HList, LabelledGeneric}
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.{Keys, SelectAll, Values}

@implicitNotFound(
  msg =
    "Cannot prove that ${T} can be projected to ${U}. Perhaps not all member names and types of ${U} are the same in ${T}?")
case class SmartProject[T: TypedEncoder, U: TypedEncoder](
    apply: TypedDataset[T] => TypedDataset[U])

object SmartProject {

  /**
   * Proofs that there is a type-safe projection from a type T to another type U. It requires
   * that: (a) both T and U are Products for which a LabelledGeneric can be derived (e.g., case
   * classes), (b) all members of U have a corresponding member in T that has both the same name
   * and type.
   *
   * @param i0
   *   the LabelledGeneric derived for T
   * @param i1
   *   the LabelledGeneric derived for U
   * @param i2
   *   the keys of U
   * @param i3
   *   selects all the values from T using the keys of U
   * @param i4
   *   selects all the values of LabeledGeneric[U]
   * @param i5
   *   proof that U and the projection of T have the same type
   * @param i6
   *   allows for traversing the keys of U
   * @tparam T
   *   the original type T
   * @tparam U
   *   the projected type U
   * @tparam TRec
   *   shapeless' Record representation of T
   * @tparam TProj
   *   the projection of T using the keys of U
   * @tparam URec
   *   shapeless' Record representation of U
   * @tparam UVals
   *   the values of U as an HList
   * @tparam UKeys
   *   the keys of U as an HList
   * @return
   *   a projection if it exists
   */
  implicit def deriveProduct[
      T: TypedEncoder,
      U: TypedEncoder,
      TRec <: HList,
      TProj <: HList,
      URec <: HList,
      UVals <: HList,
      UKeys <: HList](
      implicit i0: LabelledGeneric.Aux[T, TRec],
      i1: LabelledGeneric.Aux[U, URec],
      i2: Keys.Aux[URec, UKeys],
      i3: SelectAll.Aux[TRec, UKeys, TProj],
      i4: Values.Aux[URec, UVals],
      i5: UVals =:= TProj,
      i6: ToTraversable.Aux[UKeys, Seq, Symbol]): SmartProject[T, U] = SmartProject[T, U] {
    from =>
      val names =
        implicitly[Keys.Aux[URec, UKeys]].apply.to[Seq].map(_.name).map(from.dataset.col)
      TypedDataset.create(
        from.dataset.toDF().select(names: _*).as[U](TypedExpressionEncoder[U]))
  }
}
