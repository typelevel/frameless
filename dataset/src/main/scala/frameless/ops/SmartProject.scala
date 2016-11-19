package frameless
package ops

import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.{Keys, SelectAll, Values}
import shapeless.{HList, LabelledGeneric}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot prove that ${T} can be projected to ${U}. Perhaps not all member names and types of ${U} are the same in ${T}?")
case class SmartProject[T: TypedEncoder, U: TypedEncoder](apply: TypedDataset[T] => TypedDataset[U])

object SmartProject {
  /**
    * Proofs that there is a type-safe projection from a type T to another type U. It requires that:
    * (a) both T and U are Products for which a LabelledGeneric can be derived (e.g., case classes),
    * (b) all members of U have a corresponding member in T that has both the same name and type.
    *
    * @param tgen the LabelledGeneric derived for T
    * @param ugen the LabelledGeneric derived for U
    * @param keys the keys of U
    * @param select selects all the keys of U from T
    * @param values selects all the values of LabeledGeneric[U]
    * @param typeEqualityProof proof that U and the projection of T have the same type
    * @param keysTraverse allows for traversing the keys of U
    * @tparam T the original type T
    * @tparam U the projected type U
    * @tparam TRec shapeless' Record representation of T
    * @tparam TProj the projection of T using the keys of U
    * @tparam URec shapeless' Record representation of U
    * @tparam UVals the values of U as an HList
    * @tparam UKeys the keys of U as an HList
    * @return a projection if it exists
    */
  implicit def deriveProduct[
  T: TypedEncoder,
  U: TypedEncoder,
  TRec <: HList,
  TProj <: HList,
  URec <: HList,
  UVals <: HList,
  UKeys <: HList](
    implicit
    tgen: LabelledGeneric.Aux[T, TRec],
    ugen: LabelledGeneric.Aux[U, URec],
    keys: Keys.Aux[URec, UKeys],
    select: SelectAll.Aux[TRec, UKeys, TProj],
    values: Values.Aux[URec, UVals],
    typeEqualityProof: UVals =:= TProj,
    keysTraverse: ToTraversable.Aux[UKeys, Seq, Symbol]
  ): SmartProject[T,U] = SmartProject[T, U]( from => {
        val names = keys.apply.to[Seq].map(_.name).map(from.dataset.col)
        TypedDataset.create(from.dataset.toDF().select(names: _*).as[U](TypedExpressionEncoder[U]))
      }
    )
}
