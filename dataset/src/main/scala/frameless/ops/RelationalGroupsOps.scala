package frameless
package ops

import org.apache.spark.sql.{Column, Dataset, RelationalGroupedDataset}
import shapeless.ops.hlist.{Mapped, Prepend, ToTraversable, Tupler}
import shapeless.{::, HList, HNil, ProductArgs}

/**
  * @param groupingFunc functions used to group elements, can be cube or rollup
  * @tparam T the original `TypedDataset's` type T
  * @tparam TK all columns chosen for aggregation
  * @tparam K individual columns' types as HList
  * @tparam KT individual columns' types as Tuple
  */
private[ops] abstract class RelationalGroupsOps[T, TK <: HList, K <: HList, KT]
  (self: TypedDataset[T], groupedBy: TK, groupingFunc: (Dataset[T], Seq[Column]) => RelationalGroupedDataset)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i2: Tupler.Aux[K, KT]
  ) extends AggregatingOps(self, groupedBy, groupingFunc){

  object agg extends ProductArgs {
    /**
      * @tparam TC   resulting columns after aggregation function
      * @tparam C    individual columns' types as HList
      * @tparam OptK columns' types mapped to Option
      * @tparam Out0 OptK columns appended to C
      * @tparam Out1 output type
      */
    def applyProduct[TC <: HList, C <: HList, OptK <: HList, Out0 <: HList, Out1]
    (columns: TC)
    (implicit
      i3: AggregateTypes.Aux[T, TC, C], // shares individual columns' types after agg function as HList
      i4: Mapped.Aux[K, Option, OptK], // maps all original columns' types to Option
      i5: Prepend.Aux[OptK, C, Out0], // concatenates Option columns with those resulting from applying agg function
      i6: Tupler.Aux[Out0, Out1], // converts resulting HList into Tuple for output type
      i7: TypedEncoder[Out1], // proof that there is `TypedEncoder` for the output type
      i8: ToTraversable.Aux[TC, List, UntypedExpression[T]] // allows converting this HList to ordinary List
    ): TypedDataset[Out1] = {
      aggregate[TC, Out1](columns)
    }
  }
}

private[ops] abstract class RelationalGroups1Ops[K1, V](self: TypedDataset[V], g1: TypedColumn[V, K1]) {
  protected def underlying: RelationalGroupsOps[V, ::[TypedColumn[V, K1], HNil], ::[K1, HNil], Tuple1[K1]]
  private implicit def eg1 = g1.uencoder

  def agg[U1](c1: TypedAggregate[V, U1]): TypedDataset[(Option[K1], U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2]): TypedDataset[(Option[K1], U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3]): TypedDataset[(Option[K1], U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4]): TypedDataset[(Option[K1], U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1, c2, c3, c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4], c5: TypedAggregate[V, U5]): TypedDataset[(Option[K1], U1, U2, U3, U4, U5)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder; implicit val e5 = c5.uencoder
    underlying.agg(c1, c2, c3, c4, c5)
  }

  /** Methods on `TypedDataset[T]` that go through a full serialization and
    * deserialization of `T`, and execute outside of the Catalyst runtime.
    */
  object deserialized {
    def mapGroups[U: TypedEncoder](f: (K1, Iterator[V]) => U): TypedDataset[U] = {
      underlying.deserialized.mapGroups(AggregatingOps.tuple1(f))
    }

    def flatMapGroups[U: TypedEncoder](f: (K1, Iterator[V]) => TraversableOnce[U]): TypedDataset[U] = {
      underlying.deserialized.flatMapGroups(AggregatingOps.tuple1(f))
    }
  }

  def pivot[P: CatalystPivotable](pivotColumn: TypedColumn[V, P]): PivotNotValues[V, TypedColumn[V,K1] :: HNil, P] =
    PivotNotValues(self, g1 :: HNil, pivotColumn)
}

private[ops] abstract class RelationalGroups2Ops[K1, K2, V](self: TypedDataset[V], g1: TypedColumn[V, K1], g2: TypedColumn[V, K2]) {
  protected def underlying: RelationalGroupsOps[V, ::[TypedColumn[V, K1], ::[TypedColumn[V, K2], HNil]], ::[K1, ::[K2, HNil]], (K1, K2)]
  private implicit def eg1 = g1.uencoder
  private implicit def eg2 = g2.uencoder

  def agg[U1](c1: TypedAggregate[V, U1]): TypedDataset[(Option[K1], Option[K2], U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2]): TypedDataset[(Option[K1], Option[K2], U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3]): TypedDataset[(Option[K1], Option[K2], U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4]): TypedDataset[(Option[K1], Option[K2], U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1 , c2 , c3 , c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4], c5: TypedAggregate[V, U5]): TypedDataset[(Option[K1], Option[K2], U1, U2, U3, U4, U5)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder; implicit val e5 = c5.uencoder
    underlying.agg(c1, c2, c3, c4, c5)
  }

  /** Methods on `TypedDataset[T]` that go through a full serialization and
    * deserialization of `T`, and execute outside of the Catalyst runtime.
    */
  object deserialized {
    def mapGroups[U: TypedEncoder](f: ((K1, K2), Iterator[V]) => U): TypedDataset[U] = {
      underlying.deserialized.mapGroups(f)
    }

    def flatMapGroups[U: TypedEncoder](f: ((K1, K2), Iterator[V]) => TraversableOnce[U]): TypedDataset[U] = {
      underlying.deserialized.flatMapGroups(f)
    }
  }

  def pivot[P: CatalystPivotable](pivotColumn: TypedColumn[V, P]):
  PivotNotValues[V, TypedColumn[V,K1] :: TypedColumn[V, K2] :: HNil, P] =
    PivotNotValues(self, g1 :: g2 :: HNil, pivotColumn)
}

class RollupManyOps[T, TK <: HList, K <: HList, KT](self: TypedDataset[T], groupedBy: TK)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i2: Tupler.Aux[K, KT]
  ) extends RelationalGroupsOps[T, TK, K, KT](self, groupedBy, (dataset, cols) => dataset.rollup(cols: _*))

class Rollup1Ops[K1, V](self: TypedDataset[V], g1: TypedColumn[V, K1]) extends RelationalGroups1Ops(self, g1) {
  override protected def underlying = new RollupManyOps(self, g1 :: HNil)
}

class Rollup2Ops[K1, K2, V](self: TypedDataset[V], g1: TypedColumn[V, K1], g2: TypedColumn[V, K2]) extends RelationalGroups2Ops(self, g1, g2) {
  override protected def underlying = new RollupManyOps(self, g1 :: g2 :: HNil)
}

class CubeManyOps[T, TK <: HList, K <: HList, KT](self: TypedDataset[T], groupedBy: TK)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i2: Tupler.Aux[K, KT]
  ) extends RelationalGroupsOps[T, TK, K, KT](self, groupedBy, (dataset, cols) => dataset.cube(cols: _*))

class Cube1Ops[K1, V](self: TypedDataset[V], g1: TypedColumn[V, K1]) extends RelationalGroups1Ops(self, g1) {
  override protected def underlying = new CubeManyOps(self, g1 :: HNil)
}

class Cube2Ops[K1, K2, V](self: TypedDataset[V], g1: TypedColumn[V, K1], g2: TypedColumn[V, K2]) extends RelationalGroups2Ops(self, g1, g2) {
  override protected def underlying = new CubeManyOps(self, g1 :: g2 :: HNil)
}
