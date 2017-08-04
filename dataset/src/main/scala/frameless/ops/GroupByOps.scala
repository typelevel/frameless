package frameless
package ops

import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.plans.logical.{MapGroups, Project}
import org.apache.spark.sql.{Column, FramelessInternals}
import shapeless._
import shapeless.ops.hlist.{Prepend, ToTraversable, Tupler}

class GroupedByManyOps[T, TK <: HList, K <: HList, KT](
  self: TypedDataset[T],
  groupedBy: TK
)(
  implicit
  ct: ColumnTypes.Aux[TK, K],
  toTraversable: ToTraversable.Aux[TK, List, UntypedExpression],
  tupler: Tupler.Aux[K, KT]
) {

  object agg extends ProductArgs {
    def applyProduct[TC <: HList, C <: HList, Out0 <: HList, Out1](columns: TC)(
      implicit
      tc: AggregateTypes.Aux[T, TC, C],
      append: Prepend.Aux[K, C, Out0],
      toTuple: Tupler.Aux[Out0, Out1],
      encoder: TypedEncoder[Out1],
      columnsToList: ToTraversable.Aux[TC, List, UntypedExpression]
    ): TypedDataset[Out1] = {

      def expr(c: UntypedExpression): Column = new Column(c.expr)

      val groupByExprs = toTraversable(groupedBy).map(expr)
      val aggregates =
        if (retainGroupColumns) columnsToList(columns).map(expr)
        else groupByExprs ++ columnsToList(columns).map(expr)

      val aggregated = self.dataset.toDF()
        .groupBy(groupByExprs: _*)
        .agg(aggregates.head, aggregates.tail: _*)
        .as[Out1](TypedExpressionEncoder[Out1])

      TypedDataset.create[Out1](aggregated)
    }
  }

  def mapGroups[U: TypedEncoder](f: (KT, Iterator[T]) => U)(
    implicit kencoder: TypedEncoder[KT]
  ): TypedDataset[U] = {
    val func = (key: KT, it: Iterator[T]) => Iterator(f(key, it))
    flatMapGroups(func)
  }

  def flatMapGroups[U: TypedEncoder](
    f: (KT, Iterator[T]) => TraversableOnce[U]
  )(implicit kencoder: TypedEncoder[KT]): TypedDataset[U] = {
    implicit val tendcoder = self.encoder

    val cols = toTraversable(groupedBy)
    val logicalPlan = FramelessInternals.logicalPlan(self.dataset)
    val withKeyColumns = logicalPlan.output ++ cols.map(_.expr).map(UnresolvedAlias(_))
    val withKey = Project(withKeyColumns, logicalPlan)
    val executed = FramelessInternals.executePlan(self.dataset, withKey)
    val keyAttributes = executed.analyzed.output.takeRight(cols.size)
    val dataAttributes = executed.analyzed.output.dropRight(cols.size)

    val mapGroups = MapGroups(
      f,
      keyAttributes,
      dataAttributes,
      executed.analyzed
    )(TypedExpressionEncoder[KT], TypedExpressionEncoder[T], TypedExpressionEncoder[U])

    val groupedAndFlatMapped = FramelessInternals.mkDataset(
      self.dataset.sqlContext,
      mapGroups,
      TypedExpressionEncoder[U]
    )

    TypedDataset.create(groupedAndFlatMapped)
  }

  private def retainGroupColumns: Boolean = {
    self.dataset.sqlContext.getConf("spark.sql.retainGroupColumns", "true").toBoolean
  }
}

object GroupedByManyOps {
  /** Utility function to help Spark with serialization of closures */
  def tuple1[K1, V, U](f: (K1, Iterator[V]) => U): (Tuple1[K1], Iterator[V]) => U = {
    (x: Tuple1[K1], it: Iterator[V]) => f(x._1, it)
  }
}

class GroupedBy1Ops[K1, V](
  self: TypedDataset[V],
  g1: TypedColumn[K1]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: HNil)
  private implicit def eg1 = g1.uencoder

  def agg[U1](c1: TypedAggregate[U1]): TypedDataset[(K1, U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[U1], c2: TypedAggregate[U2]): TypedDataset[(K1, U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3]): TypedDataset[(K1, U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3], c4: TypedAggregate[U4]): TypedDataset[(K1, U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1, c2, c3, c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3], c4: TypedAggregate[U4], c5: TypedAggregate[U5]): TypedDataset[(K1, U1, U2, U3, U4, U5)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder; implicit val e5 = c5.uencoder
    underlying.agg(c1, c2, c3, c4, c5)
  }

  def mapGroups[U: TypedEncoder](f: (K1, Iterator[V]) => U): TypedDataset[U] = {
    underlying.mapGroups(GroupedByManyOps.tuple1(f))
  }

  def flatMapGroups[U: TypedEncoder](f: (K1, Iterator[V]) => TraversableOnce[U]): TypedDataset[U] = {
    underlying.flatMapGroups(GroupedByManyOps.tuple1(f))
  }
}

class GroupedBy2Ops[K1, K2, V](
  self: TypedDataset[V],
  g1: TypedColumn[K1],
  g2: TypedColumn[K2]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: g2 :: HNil)
  private implicit def eg1 = g1.uencoder
  private implicit def eg2 = g2.uencoder

  def agg[U1](c1: TypedAggregate[U1]): TypedDataset[(K1, K2, U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[U1], c2: TypedAggregate[U2]): TypedDataset[(K1, K2, U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3]): TypedDataset[(K1, K2, U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3], c4: TypedAggregate[U4]): TypedDataset[(K1, K2, U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1 , c2 , c3 , c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[U1], c2: TypedAggregate[U2], c3: TypedAggregate[U3], c4: TypedAggregate[U4], c5: TypedAggregate[U5]): TypedDataset[(K1, K2, U1, U2, U3, U4, U5)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder; implicit val e5 = c5.uencoder
    underlying.agg(c1, c2, c3, c4, c5)
  }

  def mapGroups[U: TypedEncoder](f: ((K1, K2), Iterator[V]) => U): TypedDataset[U] = {
    underlying.mapGroups(f)
  }

  def flatMapGroups[U: TypedEncoder](f: ((K1, K2), Iterator[V]) => TraversableOnce[U]): TypedDataset[U] = {
    underlying.flatMapGroups(f)
  }
}
