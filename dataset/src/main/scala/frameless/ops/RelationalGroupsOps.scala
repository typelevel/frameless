package frameless
package ops

import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.plans.logical.{MapGroups, Project}
import org.apache.spark.sql.{Column, Dataset, FramelessInternals, RelationalGroupedDataset}
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
  ) {
  object agg extends ProductArgs {
    /**
      * @param i3 shares individual columns' types
      * @param i4 maps all types in HList to Option
      * @param i5 concatenates two HLists
      * @param i6 converts HList to Tuple
      * @param i7 proof that there is `TypedEncoder` for the output type
      * @param i8 allows converting thi HList to ordinary List
      * @tparam TC resulting columns after aggregation function
      * @tparam C individual columns' types as HList
      * @tparam OptK columns' types mapped to Option
      * @tparam Out0 OptK columns appended to C
      * @tparam Out1 output type
      */
    def applyProduct[TC <: HList, C <: HList, OptK <: HList, Out0 <: HList, Out1]
    (columns: TC)
    (implicit
     i3: AggregateTypes.Aux[T, TC, C],
     i4: Mapped.Aux[K, Option, OptK],
     i5: Prepend.Aux[OptK, C, Out0],
     i6: Tupler.Aux[Out0, Out1],
     i7: TypedEncoder[Out1],
     i8: ToTraversable.Aux[TC, List, UntypedExpression[T]]
    ): TypedDataset[Out1] = {
      def expr(c: UntypedExpression[T]): Column = new Column(c.expr)

      val groupByExprs = groupedBy.toList[UntypedExpression[T]].map(expr)
      val aggregates =
        if (retainGroupColumns) columns.toList[UntypedExpression[T]].map(expr)
        else groupByExprs ++ columns.toList[UntypedExpression[T]].map(expr)

      val aggregated =
        groupingFunc(self.dataset, groupByExprs)
          .agg(aggregates.head, aggregates.tail: _*)
          .as[Out1](TypedExpressionEncoder[Out1])

      TypedDataset.create[Out1](aggregated)
    }
  }

  /** Methods on `TypedDataset[T]` that go through a full serialization and
    * deserialization of `T`, and execute outside of the Catalyst runtime.
    */
  object deserialized {
    def mapGroups[U: TypedEncoder](f: (KT, Iterator[T]) => U)(
      implicit e: TypedEncoder[KT]
    ): TypedDataset[U] = {
      val func = (key: KT, it: Iterator[T]) => Iterator(f(key, it))
      flatMapGroups(func)
    }

    def flatMapGroups[U: TypedEncoder](f: (KT, Iterator[T]) => TraversableOnce[U])
                                      (implicit e: TypedEncoder[KT]): TypedDataset[U] = {
      implicit val tendcoder = self.encoder

      val cols = groupedBy.toList[UntypedExpression[T]]
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
  }

  private def retainGroupColumns: Boolean = {
    self.dataset.sqlContext.getConf("spark.sql.retainGroupColumns", "true").toBoolean
  }

  def pivot[P: CatalystPivotable](pivotColumn: TypedColumn[T, P]):
  PivotNotValues[T, TK, P] =
    PivotNotValues(self, groupedBy, pivotColumn)
}

private[ops] object RelationalGroupsOps {
  /** Utility function to help Spark with serialization of closures */
  def tuple1[K1, V, U](f: (K1, Iterator[V]) => U): (Tuple1[K1], Iterator[V]) => U = {
    (x: Tuple1[K1], it: Iterator[V]) => f(x._1, it)
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
      underlying.deserialized.mapGroups(RelationalGroupsOps.tuple1(f))
    }

    def flatMapGroups[U: TypedEncoder](f: (K1, Iterator[V]) => TraversableOnce[U]): TypedDataset[U] = {
      underlying.deserialized.flatMapGroups(RelationalGroupsOps.tuple1(f))
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