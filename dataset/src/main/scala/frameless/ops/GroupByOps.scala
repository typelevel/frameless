package frameless
package ops

import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.plans.logical.{MapGroups, Project}
import org.apache.spark.sql.{Column, Dataset, FramelessInternals, RelationalGroupedDataset}
import shapeless._
import shapeless.ops.hlist.{Length, Mapped, Prepend, ToList, ToTraversable, Tupler}

class GroupedByManyOps[T, TK <: HList, K <: HList, KT]
  (self: TypedDataset[T], groupedBy: TK)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i3: Tupler.Aux[K, KT]
  ) extends AggregatingOps[T, TK, K, KT](self, groupedBy, (dataset, cols) => dataset.groupBy(cols: _*)) {
  object agg extends ProductArgs {
    def applyProduct[TC <: HList, C <: HList, Out0 <: HList, Out1]
      (columns: TC)
      (implicit
        i3: AggregateTypes.Aux[T, TC, C],
        i4: Prepend.Aux[K, C, Out0],
        i5: Tupler.Aux[Out0, Out1],
        i6: TypedEncoder[Out1],
        i7: ToTraversable.Aux[TC, List, UntypedExpression[T]]
      ): TypedDataset[Out1] = {
        aggregate[TC, Out1](columns)
      }
  }
}

class GroupedBy1Ops[K1, V](
  self: TypedDataset[V],
  g1: TypedColumn[V, K1]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: HNil)
  private implicit def eg1 = g1.uencoder

  def agg[U1](c1: TypedAggregate[V, U1]): TypedDataset[(K1, U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2]): TypedDataset[(K1, U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3]): TypedDataset[(K1, U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4]): TypedDataset[(K1, U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1, c2, c3, c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4], c5: TypedAggregate[V, U5]): TypedDataset[(K1, U1, U2, U3, U4, U5)] = {
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


class GroupedBy2Ops[K1, K2, V](
  self: TypedDataset[V],
  g1: TypedColumn[V, K1],
  g2: TypedColumn[V, K2]
) {
  private def underlying = new GroupedByManyOps(self, g1 :: g2 :: HNil)
  private implicit def eg1 = g1.uencoder
  private implicit def eg2 = g2.uencoder

  def agg[U1](c1: TypedAggregate[V, U1]): TypedDataset[(K1, K2, U1)] = {
    implicit val e1 = c1.uencoder
    underlying.agg(c1)
  }

  def agg[U1, U2](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2]): TypedDataset[(K1, K2, U1, U2)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder
    underlying.agg(c1, c2)
  }

  def agg[U1, U2, U3](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3]): TypedDataset[(K1, K2, U1, U2, U3)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder
    underlying.agg(c1, c2, c3)
  }

  def agg[U1, U2, U3, U4](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4]): TypedDataset[(K1, K2, U1, U2, U3, U4)] = {
    implicit val e1 = c1.uencoder; implicit val e2 = c2.uencoder; implicit val e3 = c3.uencoder; implicit val e4 = c4.uencoder
    underlying.agg(c1 , c2 , c3 , c4)
  }

  def agg[U1, U2, U3, U4, U5](c1: TypedAggregate[V, U1], c2: TypedAggregate[V, U2], c3: TypedAggregate[V, U3], c4: TypedAggregate[V, U4], c5: TypedAggregate[V, U5]): TypedDataset[(K1, K2, U1, U2, U3, U4, U5)] = {
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

private[ops] abstract class AggregatingOps[T, TK <: HList, K <: HList, KT]
  (self: TypedDataset[T], groupedBy: TK, groupingFunc: (Dataset[T], Seq[Column]) => RelationalGroupedDataset)
  (implicit
    i0: ColumnTypes.Aux[T, TK, K],
    i1: ToTraversable.Aux[TK, List, UntypedExpression[T]],
    i2: Tupler.Aux[K, KT]
  ) {
  def aggregate[TC <: HList, Out1](columns: TC)
  (implicit
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

  /** Methods on `TypedDataset[T]` that go through a full serialization and
    * deserialization of `T`, and execute outside of the Catalyst runtime.
    */
  object deserialized {
    def mapGroups[U: TypedEncoder](
      f: (KT, Iterator[T]) => U
    )(implicit e: TypedEncoder[KT]): TypedDataset[U] = {
      val func = (key: KT, it: Iterator[T]) => Iterator(f(key, it))
      flatMapGroups(func)
    }

    def flatMapGroups[U: TypedEncoder](
      f: (KT, Iterator[T]) => TraversableOnce[U]
    )(implicit e: TypedEncoder[KT]): TypedDataset[U] = {
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

  def pivot[P: CatalystPivotable](pivotColumn: TypedColumn[T, P]): PivotNotValues[T, TK, P] =
    PivotNotValues(self, groupedBy, pivotColumn)
}

private[ops] object AggregatingOps {
  /** Utility function to help Spark with serialization of closures */
  def tuple1[K1, V, U](f: (K1, Iterator[V]) => U): (Tuple1[K1], Iterator[V]) => U = {
    (x: Tuple1[K1], it: Iterator[V]) => f(x._1, it)
  }
}

/** Represents a typed Pivot operation.
  */
final case class Pivot[T, GroupedColumns <: HList, PivotType, Values <: HList](
  ds: TypedDataset[T],
  groupedBy: GroupedColumns,
  pivotedBy: TypedColumn[T, PivotType],
  values: Values
) {

  object agg extends ProductArgs {
    def applyProduct[AggrColumns <: HList, AggrColumnTypes <: HList, GroupedColumnTypes <: HList, NumValues <: Nat, TypesForPivotedValues <: HList, TypesForPivotedValuesOpt <: HList, OutAsHList <: HList, Out]
      (aggrColumns: AggrColumns)
      (implicit
        i0: AggregateTypes.Aux[T, AggrColumns, AggrColumnTypes],
        i1: ColumnTypes.Aux[T, GroupedColumns, GroupedColumnTypes],
        i2: Length.Aux[Values, NumValues],
        i3: Repeat.Aux[AggrColumnTypes, NumValues, TypesForPivotedValues],
        i4: Mapped.Aux[TypesForPivotedValues, Option, TypesForPivotedValuesOpt],
        i5: Prepend.Aux[GroupedColumnTypes, TypesForPivotedValuesOpt, OutAsHList],
        i6: Tupler.Aux[OutAsHList, Out],
        i7: TypedEncoder[Out]
      ): TypedDataset[Out] = {
        def mapAny[X](h: HList)(f: Any => X): List[X] =
          h match {
            case HNil    => Nil
            case x :: xs => f(x) :: mapAny(xs)(f)
          }

        val aggCols: Seq[Column] = mapAny(aggrColumns)(x => new Column(x.asInstanceOf[TypedAggregate[_,_]].expr))
        val tmp = ds.dataset.toDF()
          .groupBy(mapAny(groupedBy)(_.asInstanceOf[TypedColumn[_, _]].untyped): _*)
          .pivot(pivotedBy.untyped.toString, mapAny(values)(identity))
          .agg(aggCols.head, aggCols.tail:_*)
          .as[Out](TypedExpressionEncoder[Out])
        TypedDataset.create(tmp)
      }
  }
}

final case class PivotNotValues[T, GroupedColumns <: HList, PivotType](
  ds: TypedDataset[T],
  groupedBy: GroupedColumns,
  pivotedBy: TypedColumn[T, PivotType]
) extends ProductArgs {

  def onProduct[Values <: HList](values: Values)(
    implicit validValues: ToList[Values, PivotType] // validValues: FilterNot.Aux[Values, PivotType, HNil] // did not work
  ): Pivot[T, GroupedColumns, PivotType, Values] = Pivot(ds, groupedBy, pivotedBy, values)
}
