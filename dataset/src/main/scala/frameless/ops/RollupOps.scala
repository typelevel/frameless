package frameless.ops

import frameless._
import shapeless.ops.hlist.{ToTraversable, Tupler}
import shapeless.{HList, HNil}

class RollupManyOps[T, TK <: HList, K <: HList, KT](self: TypedDataset[T], groupedBy: TK)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i3: Tupler.Aux[K, KT]
  ) extends RelationalGroupsOps[T, TK, K, KT](self, groupedBy, (dataset, cols) => dataset.rollup(cols: _*))

class Rollup1Ops[K1, V](self: TypedDataset[V], g1: TypedColumn[V, K1]) extends RelationalGroups1Ops(self, g1) {
  override protected def underlying = new RollupManyOps(self, g1 :: HNil)
}

class Rollup2Ops[K1, K2, V](self: TypedDataset[V], g1: TypedColumn[V, K1], g2: TypedColumn[V, K2]) extends RelationalGroups2Ops(self, g1, g2) {
  override protected def underlying = new RollupManyOps(self, g1 :: g2 :: HNil)
}