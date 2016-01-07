package typedframe

import shapeless._
import shapeless.ops.record.{Keys, SelectAll}
import shapeless.ops.hlist.ToList

/** Type class aggregating `LabelledGeneric`, `record.Keys` and `hlist.ToList`
  * to extract the name of all fields of a product. */
trait Fields[S <: Product] extends Serializable {
  def apply(): Seq[String]
}

object Fields {
  def apply[S <: Product](implicit f: Fields[S]): Fields[S] = f
  
  implicit def mkFields[S <: Product, R <: HList, K <: HList]
    (implicit
      g: LabelledGeneric.Aux[S, R],
      k: Keys.Aux[R, K],
      t: ToList[K, Symbol]
    ): Fields[S] =
      new Fields[S] {
        def apply() = k().toList.map(_.name)
      }
}

/** Type class witnessing that a set of symbols are fields of a product.
  * Aggregates `ToList`, `LabelledGeneric` and `SelectAll` to extract field names. */
trait FieldsExtractor[S <: Product, C <: HList] extends Serializable {
  type Repr <: HList
  type Fields <: HList
  def apply(fields: C): Seq[String]
}

object FieldsExtractor {
  def apply[S <: Product, C <: HList](implicit f: FieldsExtractor[S, C]): FieldsExtractor[S, C] = f
  
  type Aux[S <: Product, C <: HList, R0 <: HList, F0 <: HList] =
    FieldsExtractor[S, C] { type Repr = R0; type Fields = F0 }
  
  implicit def mkFieldsExtractor[S <: Product, C <: HList, R <: HList, F <: HList]
    (implicit
      t: ToList[C, Symbol],
      g: LabelledGeneric.Aux[S, R],
      s: SelectAll.Aux[R, C, F]
    ): Aux[S, C, R, F] =
      new FieldsExtractor[S, C] {
        type Repr = R
        type Fields = F
        def apply(fields: C): Seq[String] = fields.toList.map(_.name)
      }
}
