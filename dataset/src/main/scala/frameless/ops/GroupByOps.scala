package frameless
package ops

import org.apache.spark.sql.Column

import shapeless._
import shapeless.ops.hlist.{Tupler, ToTraversable, Prepend}

class GroupedByManyOps[T, TK <: HList, K <: HList](
  self: TypedDataset[T],
  groupedBy: TK
)(
  implicit
  ct: ColumnTypes.Aux[T, TK, K],
  toTraversable: ToTraversable.Aux[TK, List, UntypedColumn[T]]
) {

  def agg[TC <: HList, C <: HList, Out0 <: HList, Out1](columns: TC)(
    implicit
    tcColumnType: ColumnTypes.Aux[T, TC, C],
    encoder: TypedEncoder[Out1],
    append: Prepend.Aux[K, C, Out0],
    toTuple: Tupler.Aux[Out0, Out1],
    columnsToList: ToTraversable.Aux[TC, List, UntypedColumn[T]]
  ): TypedDataset[Out1] = {

    def expr(c: UntypedColumn[T]): Column = new Column(c.expr)

    val groupByExprs = toTraversable(groupedBy).map(expr)
    val aggregates =
      if (retainGroupColumns) columnsToList(columns).map(expr)
      else groupByExprs ++ columnsToList(columns).map(expr)

    val aggregated = self.dataset.toDF()
      .groupBy(groupByExprs: _*)
      .agg(aggregates.head, aggregates.tail: _*)
      .as[Out1](TypedExpressionEncoder[Out1])

    new TypedDataset[Out1](aggregated)
  }

  private def retainGroupColumns: Boolean = {
    self.dataset.sqlContext.getConf("spark.sql.retainGroupColumns", "true").toBoolean
  }
}

class GroupedBy1Ops[K1, V](
  self: TypedDataset[V],
  g1: TypedColumn[V, K1]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: HNil)

  def agg[U1](c1: TypedColumn[V, U1])(
    implicit encoder: TypedEncoder[(K1, U1)]
  ): TypedDataset[(K1, U1)] = underlying.agg(c1 :: HNil)

  def agg[U1, U2](c1: TypedColumn[V, U1], c2: TypedColumn[V, U2])(
    implicit encoder: TypedEncoder[(K1, U1, U2)]
  ): TypedDataset[(K1, U1, U2)] = underlying.agg(c1 :: c2 :: HNil)

  def agg[U1, U2, U3](c1: TypedColumn[V, U1], c2: TypedColumn[V, U2], c3: TypedColumn[V, U3])(
    implicit encoder: TypedEncoder[(K1, U1, U2, U3)]
  ): TypedDataset[(K1, U1, U2, U3)] = underlying.agg(c1 :: c2 :: c3 :: HNil)
}

class GroupedBy2Ops[K1, K2, V](
  self: TypedDataset[V],
  g1: TypedColumn[V, K1],
  g2: TypedColumn[V, K2]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: g2 :: HNil)

  def agg[U1](c1: TypedColumn[V, U1])(
    implicit encoder: TypedEncoder[(K1, K2, U1)]
  ): TypedDataset[(K1, K2, U1)] = underlying.agg(c1 :: HNil)

  def agg[U1, U2](c1: TypedColumn[V, U1], c2: TypedColumn[V, U2])(
    implicit encoder: TypedEncoder[(K1, K2, U1, U2)]
  ): TypedDataset[(K1, K2, U1, U2)] = underlying.agg(c1 :: c2 :: HNil)

  def agg[U1, U2, U3](c1: TypedColumn[V, U1], c2: TypedColumn[V, U2], c3: TypedColumn[V, U3])(
    implicit encoder: TypedEncoder[(K1, K2, U1, U2, U3)]
  ): TypedDataset[(K1, K2, U1, U2, U3)] = underlying.agg(c1 :: c2 :: c3 :: HNil)
}
