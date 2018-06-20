package frameless

import java.util

import frameless.functions.CatalystExplodableCollection
import frameless.ops._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.types.StructType
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Diff, IsHCons, Mapper, Prepend, ToTraversable, Tupler}
import shapeless.ops.record.{Keys, Modifier, Remover, Values}

/** [[TypedDataset]] is a safer interface for working with `Dataset`.
  *
  * NOTE: Prefer `TypedDataset.create` over `new TypedDataset` unless you
  * know what you are doing.
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
class TypedDataset[T] protected[frameless](val dataset: Dataset[T])(implicit val encoder: TypedEncoder[T])
    extends TypedDatasetForwarded[T] { self =>

  private implicit val spark: SparkSession = dataset.sparkSession

  /** Aggregates on the entire Dataset without groups.
    *
    * apache/spark
    */
  def agg[A](ca: TypedAggregate[T, A]): TypedDataset[A] = {
    implicit val ea = ca.uencoder
    val tuple1: TypedDataset[Tuple1[A]] = aggMany(ca)

    // now we need to unpack `Tuple1[A]` to `A`
    TypedEncoder[A].catalystRepr match {
      case StructType(_) =>
        // if column is struct, we use all its fields
        val df = tuple1
          .dataset
          .selectExpr("_1.*")
          .as[A](TypedExpressionEncoder[A])

        TypedDataset.create(df)
      case other =>
        // for primitive types `Tuple1[A]` has the same schema as `A`
        TypedDataset.create(tuple1.dataset.as[A](TypedExpressionEncoder[A]))
    }
  }

  /** Aggregates on the entire Dataset without groups.
    *
    * apache/spark
    */
  def agg[A, B](
    ca: TypedAggregate[T, A],
    cb: TypedAggregate[T, B]
  ): TypedDataset[(A, B)] = {
    implicit val (ea, eb) = (ca.uencoder, cb.uencoder)
    aggMany(ca, cb)
  }

  /** Aggregates on the entire Dataset without groups.
    *
    * apache/spark
    */
  def agg[A, B, C](
    ca: TypedAggregate[T, A],
    cb: TypedAggregate[T, B],
    cc: TypedAggregate[T, C]
  ): TypedDataset[(A, B, C)] = {
    implicit val (ea, eb, ec) = (ca.uencoder, cb.uencoder, cc.uencoder)
    aggMany(ca, cb, cc)
  }

  /** Aggregates on the entire Dataset without groups.
    *
    * apache/spark
    */
  def agg[A, B, C, D](
    ca: TypedAggregate[T, A],
    cb: TypedAggregate[T, B],
    cc: TypedAggregate[T, C],
    cd: TypedAggregate[T, D]
  ): TypedDataset[(A, B, C, D)] = {
    implicit val (ea, eb, ec, ed) = (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder)
    aggMany(ca, cb, cc, cd)
  }

  /** Aggregates on the entire Dataset without groups.
    *
    * apache/spark
    */
  object aggMany extends ProductArgs {
    def applyProduct[U <: HList, Out0 <: HList, Out](columns: U)
      (implicit
        i0: AggregateTypes.Aux[T, U, Out0],
        i1: ToTraversable.Aux[U, List, UntypedExpression[T]],
        i2: Tupler.Aux[Out0, Out],
        i3: TypedEncoder[Out]
      ): TypedDataset[Out] = {

      val underlyingColumns = columns.toList[UntypedExpression[T]]
      val cols: Seq[Column] = for {
        (c, i) <- columns.toList[UntypedExpression[T]].zipWithIndex
      } yield new Column(c.expr).as(s"_${i+1}")

      // Workaround to SPARK-20346. One alternative is to allow the result to be Vector(null) for empty DataFrames.
      // Another one would be to return an Option.
      val filterStr = (
        for {
          (c, i) <- underlyingColumns.zipWithIndex
          if !c.uencoder.nullable
        } yield s"_${i+1} is not null"
        ).mkString(" or ")

      val selected = dataset.toDF().agg(cols.head, cols.tail:_*).as[Out](TypedExpressionEncoder[Out])
      TypedDataset.create[Out](if (filterStr.isEmpty) selected else selected.filter(filterStr))
    }
  }

  /** Returns a new [[TypedDataset]] where each record has been mapped on to the specified type. */
  def as[U]()(implicit as: As[T, U]): TypedDataset[U] = {
    implicit val uencoder = as.encoder
    TypedDataset.create(dataset.as[U](TypedExpressionEncoder[U]))
  }

  /** Returns a checkpointed version of this [[TypedDataset]]. Checkpointing can be used to truncate the
    * logical plan of this Dataset, which is especially useful in iterative algorithms where the
    * plan may grow exponentially. It will be saved to files inside the checkpoint
    * directory set with `SparkContext#setCheckpointDir`.
    *
    * Differs from `Dataset#checkpoint` by wrapping its result into an effect-suspending `F[_]`.
    *
    * apache/spark
    */
  def checkpoint[F[_]](eager: Boolean)(implicit F: SparkDelay[F]): F[TypedDataset[T]] =
    F.delay(TypedDataset.create[T](dataset.checkpoint(eager)))

  /** Returns a new [[TypedDataset]] where each record has been mapped on to the specified type.
    * Unlike `as` the projection U may include a subset of the columns of T and the column names and types must agree.
    *
    * {{{
    *   case class Foo(i: Int, j: String)
    *   case class Bar(j: String)
    *
    *   val t: TypedDataset[Foo] = ...
    *   val b: TypedDataset[Bar] = t.project[Bar]
    *
    *   case class BarErr(e: String)
    *   // The following does not compile because `Foo` doesn't have a field with name `e`
    *   val e: TypedDataset[BarErr] = t.project[BarErr]
    * }}}
    */
  def project[U](implicit projector: SmartProject[T,U]): TypedDataset[U] = projector.apply(this)

  /** Returns a new [[TypedDataset]] that contains the elements of both this and the `other` [[TypedDataset]]
    * combined.
    *
    * Note that, this function is not a typical set union operation, in that it does not eliminate
    * duplicate items. As such, it is analogous to `UNION ALL` in SQL.
    *
    * Differs from `Dataset#union` by aligning fields if possible.
    * It will not compile if `Datasets` have not compatible schema.
    *
    * Example:
    * {{{
    *   case class Foo(x: Int, y: Long)
    *   case class Bar(y: Long, x: Int)
    *   case class Faz(x: Int, y: Int, z: Int)
    *
    *   foo: TypedDataset[Foo] = ...
    *   bar: TypedDataset[Bar] = ...
    *   faz: TypedDataset[Faz] = ...
    *
    *   foo union bar: TypedDataset[Foo]
    *   foo union faz: TypedDataset[Foo]
    *   // won't compile, you need to reverse order, you can't project from less fields to more
    *   faz union foo
    *
    * }}}
    *
    * apache/spark
    */
  def union[U: TypedEncoder](other: TypedDataset[U])(implicit projector: SmartProject[U, T]): TypedDataset[T] =
    TypedDataset.create(dataset.union(other.project[T].dataset))

  /** Returns a new [[TypedDataset]] that contains the elements of both this and the `other` [[TypedDataset]]
    * combined.
    *
    * Note that, this function is not a typical set union operation, in that it does not eliminate
    * duplicate items. As such, it is analogous to `UNION ALL` in SQL.
    *
    * apache/spark
    */
  def union(other: TypedDataset[T]): TypedDataset[T] = {
    TypedDataset.create(dataset.union(other.dataset))
  }

  /** Returns the number of elements in the [[TypedDataset]].
    *
    * Differs from `Dataset#count` by wrapping its result into an effect-suspending `F[_]`.
    */
  def count[F[_]]()(implicit F: SparkDelay[F]): F[Long] =
    F.delay(dataset.count)

  /** Returns `TypedColumn` of type `A` given its name.
    *
    * {{{
    * tf('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def apply[A, Tw](column: Witness.Lt[Symbol])
    (implicit
      i0: SchemaWrapper.Aux[T, Tw],
      i1: TypedColumn.Exists[Tw, column.T, A],
      i2: TypedEncoder[A]
    ): TypedColumn[T, A] = col(column)

  /** Returns `TypedColumn` of type `A` given its name.
    *
    * {{{
    * tf.col('id)
    * }}}
  *
  * It is statically checked that column with such name exists and has type `A`.
    */
  def col[A, Tw](column: Witness.Lt[Symbol])
    (implicit
      i0: SchemaWrapper.Aux[T, Tw],
      i1: TypedColumn.Exists[Tw, column.T, A],
      i2: TypedEncoder[A]
    ): TypedColumn[T, A] =
    new TypedColumn[T, A](dataset(column.value.name).as[A](TypedExpressionEncoder[A]))

  /** Projects the entire TypedDataset[T] into a single column of type TypedColumn[T,T]
    * {{{
    *   ts: TypedDataset[Foo] = ...
    *   ts.select(ts.asCol, ts.asCol): TypedDataset[(Foo,Foo)]
    * }}}
    */
  def asCol: TypedColumn[T, T] = {
    val allColumns: Array[Column] = dataset.columns.map(dataset.col)
    val projectedColumn: Column = org.apache.spark.sql.functions.struct(allColumns: _*)
    new TypedColumn[T,T](projectedColumn)
  }

  object colMany extends SingletonProductArgs {
    def applyProduct[U <: HList, Out, Tw](columns: U)
      (implicit
        i0: SchemaWrapper.Aux[T, Tw],
        i1: TypedColumn.ExistsMany[Tw, U, Out],
        i2: TypedEncoder[Out],
        i3: ToTraversable.Aux[U, List, Symbol]): TypedColumn[T, Out] = {
      val names = columns.toList[Symbol].map(_.name)
      val colExpr = FramelessInternals.resolveExpr(dataset, names)
      new TypedColumn[T, Out](colExpr)
    }
  }

  /** Right hand side disambiguation of `col` for join expressions.
    * To be used  when writting self-joins, noop in other circumstances.
    *
    * Note: In vanilla Spark, disambiguation in self-joins is acheaved using
    * String based aliases, which is obviously unsafe.
    */
  def colRight[A, Tw](column: Witness.Lt[Symbol])
    (implicit
      i0: SchemaWrapper.Aux[T, Tw],
      i1: TypedColumn.Exists[Tw, column.T, A],
      i2: TypedEncoder[A]): TypedColumn[T, A] =
    new TypedColumn[T, A](FramelessInternals.DisambiguateRight(col(column).expr))

  /** Left hand side disambiguation of `col` for join expressions.
    * To be used  when writting self-joins, noop in other circumstances.
    *
    * Note: In vanilla Spark, disambiguation in self-joins is acheaved using
    * String based aliases, which is obviously unsafe.
    */
  def colLeft[A, Tw](column: Witness.Lt[Symbol])
    (implicit
      i0: SchemaWrapper.Aux[T, Tw],
      i1: TypedColumn.Exists[Tw, column.T, A],
      i2: TypedEncoder[A]): TypedColumn[T, A] =
    new TypedColumn[T, A](FramelessInternals.DisambiguateLeft(col(column).expr))

  /** Returns a `Seq` that contains all the elements in this [[TypedDataset]].
    *
    * Running this operation requires moving all the data into the application's driver process, and
    * doing so on a very large [[TypedDataset]] can crash the driver process with OutOfMemoryError.
    *
    * Differs from `Dataset#collect` by wrapping its result into an effect-suspending `F[_]`.
    */
  def collect[F[_]]()(implicit F: SparkDelay[F]): F[Seq[T]] =
    F.delay(dataset.collect())

  /** Optionally returns the first element in this [[TypedDataset]].
    *
    * Differs from `Dataset#first` by wrapping its result into an `Option` and an effect-suspending `F[_]`.
    */
  def firstOption[F[_]]()(implicit F: SparkDelay[F]): F[Option[T]] =
    F.delay {
      try {
        Option(dataset.first())
      } catch {
        case e: NoSuchElementException => None
      }
    }

  /** Returns the first `num` elements of this [[TypedDataset]] as a `Seq`.
    *
    * Running take requires moving data into the application's driver process, and doing so with
    * a very large `num` can crash the driver process with OutOfMemoryError.
    *
    * Differs from `Dataset#take` by wrapping its result into an effect-suspending `F[_]`.
    *
    * apache/spark
    */
  def take[F[_]](num: Int)(implicit F: SparkDelay[F]): F[Seq[T]] =
    F.delay(dataset.take(num))

  /** Return an iterator that contains all rows in this [[TypedDataset]].
    *
    * The iterator will consume as much memory as the largest partition in this [[TypedDataset]].
    *
    * NOTE: this results in multiple Spark jobs, and if the input [[TypedDataset]] is the result
    * of a wide transformation (e.g. join with different partitioners), to avoid
    * recomputing the input [[TypedDataset]] should be cached first.
    *
    * Differs from `Dataset#toLocalIterator()` by wrapping its result into an effect-suspending `F[_]`.
    *
    * apache/spark
    */
  def toLocalIterator[F[_]]()(implicit F: SparkDelay[F]): F[util.Iterator[T]] =
    F.delay(dataset.toLocalIterator())

  /** Alias for firstOption().
    */
  def headOption[F[_]]()(implicit F: SparkDelay[F]): F[Option[T]] = firstOption()

  /** Alias for take().
    */
  def head[F[_]](num: Int)(implicit F: SparkDelay[F]): F[Seq[T]] = take(num)

  // $COVERAGE-OFF$
  /** Alias for firstOption().
    */
  @deprecated("Method may throw exception. Use headOption or firstOption instead.", "0.5.0")
  def head: T = dataset.head()

  /** Alias for firstOption().
    */
  @deprecated("Method may throw exception. Use headOption or firstOption instead.", "0.5.0")
  def first: T = dataset.head()
  // $COVERAGE-ONN$

  /** Displays the content of this [[TypedDataset]] in a tabular form. Strings more than 20 characters
    * will be truncated, and all cells will be aligned right. For example:
    * {{{
    *   year  month AVG('Adj Close) MAX('Adj Close)
    *   1980  12    0.503218        0.595103
    *   1981  01    0.523289        0.570307
    *   1982  02    0.436504        0.475256
    *   1983  03    0.410516        0.442194
    *   1984  04    0.450090        0.483521
    * }}}
    * @param numRows Number of rows to show
    * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
    *   be truncated and all cells will be aligned right
    *
    * Differs from `Dataset#show` by wrapping its result into an effect-suspending `F[_]`.
    *
    * apache/spark
    */
  def show[F[_]](numRows: Int = 20, truncate: Boolean = true)(implicit F: SparkDelay[F]): F[Unit] =
    F.delay(dataset.show(numRows, truncate))

  /** Returns a new [[frameless.TypedDataset]] that only contains elements where `column` is `true`.
    *
    * Differs from `TypedDatasetForward#filter` by taking a `TypedColumn[T, Boolean]` instead of a
    * `T => Boolean`. Using a column expression instead of a regular function save one Spark → Scala
    * deserialization which leads to better performance.
    */
  def filter(column: TypedColumn[T, Boolean]): TypedDataset[T] = {
    val filtered = dataset.toDF()
      .filter(column.untyped)
      .as[T](TypedExpressionEncoder[T])

    TypedDataset.create[T](filtered)
  }

  /** Runs `func` on each element of this [[TypedDataset]].
    *
    * Differs from `Dataset#foreach` by wrapping its result into an effect-suspending `F[_]`.
    */
  def foreach[F[_]](func: T => Unit)(implicit F: SparkDelay[F]): F[Unit] =
    F.delay(dataset.foreach(func))

  /** Runs `func` on each partition of this [[TypedDataset]].
    *
    * Differs from `Dataset#foreachPartition` by wrapping its result into an effect-suspending `F[_]`.
    */
  def foreachPartition[F[_]](func: Iterator[T] => Unit)(implicit F: SparkDelay[F]): F[Unit] =
    F.delay(dataset.foreachPartition(func))

  /**
    * Create a multi-dimensional cube for the current [[TypedDataset]] using the specified column,
    * so we can run aggregation on it.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * Differs from `Dataset#cube` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  def cube[K1](
    c1: TypedColumn[T, K1]
  ): Cube1Ops[K1, T] = new Cube1Ops[K1, T](this, c1)

  /**
    * Create a multi-dimensional cube for the current [[TypedDataset]] using the specified columns,
    * so we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * Differs from `Dataset#cube` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  def cube[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): Cube2Ops[K1, K2, T] = new Cube2Ops[K1, K2, T](this, c1, c2)

  /**
    * Create a multi-dimensional cube for the current [[TypedDataset]] using the specified columns,
    * so we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * {{{
    *   case class MyClass(a: Int, b: Int, c: Int)
    *   val ds: TypedDataset[MyClass]

    *   val cubeDataset: TypedDataset[(Option[A], Option[B], Long)] =
    *     ds.cubeMany(ds('a), ds('b)).agg(count[MyClass]())
    *
    *   // original dataset:
    *     a       b     c
    *    10      20     1
    *    15      25     2
    *
    *   // after aggregation:
    *     _1      _2   _3
    *     15    null    1
    *     15      25    1
    *   null    null    2
    *   null      25    1
    *   null      20    1
    *     10    null    1
    *     10      20    1
    *
    * }}}
    *
    * Differs from `Dataset#cube` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  object cubeMany extends ProductArgs {
    def applyProduct[TK <: HList, K <: HList, KT](groupedBy: TK)
      (implicit
        i0: ColumnTypes.Aux[T, TK, K],
        i1: Tupler.Aux[K, KT],
        i2: ToTraversable.Aux[TK, List, UntypedExpression[T]]
      ): CubeManyOps[T, TK, K, KT] = new CubeManyOps[T, TK, K, KT](self, groupedBy)
  }

  /**
    * Groups the [[TypedDataset]] using the specified columns, so that we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * apache/spark
    */
  def groupBy[K1](
    c1: TypedColumn[T, K1]
  ): GroupedBy1Ops[K1, T] = new GroupedBy1Ops[K1, T](this, c1)

  /**
    * Groups the [[TypedDataset]] using the specified columns, so that we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * apache/spark
    */
  def groupBy[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): GroupedBy2Ops[K1, K2, T] = new GroupedBy2Ops[K1, K2, T](this, c1, c2)

  /**
    * Groups the [[TypedDataset]] using the specified columns, so that we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * {{{
    *   case class MyClass(a: Int, b: Int, c: Int)
    *   val ds: TypedDataset[MyClass]
    *
    *   val cubeDataset: TypedDataset[(Option[A], Option[B], Long)] =
    *     ds.groupByMany(ds('a), ds('b)).agg(count[MyClass]())
    *
    *   // original dataset:
    *     a       b     c
    *    10      20     1
    *    15      25     2
    *
    *   // after aggregation:
    *     _1      _2   _3
    *     10      20    1
    *     15      25    1
    *
    * }}}
    *
    * apache/spark
    */
  object groupByMany extends ProductArgs {
    def applyProduct[TK <: HList, K <: HList, KT](groupedBy: TK)
      (implicit
        i0: ColumnTypes.Aux[T, TK, K],
        i1: Tupler.Aux[K, KT],
        i2: ToTraversable.Aux[TK, List, UntypedExpression[T]]
      ): GroupedByManyOps[T, TK, K, KT] = new GroupedByManyOps[T, TK, K, KT](self, groupedBy)
  }

  /**
    * Create a multi-dimensional rollup for the current [[TypedDataset]] using the specified column,
    * so we can run aggregation on it.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * Differs from `Dataset#rollup` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  def rollup[K1](
    c1: TypedColumn[T, K1]
  ): Rollup1Ops[K1, T] = new Rollup1Ops[K1, T](this, c1)

  /**
    * Create a multi-dimensional rollup for the current [[TypedDataset]] using the specified columns,
    * so we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * Differs from `Dataset#rollup` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  def rollup[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): Rollup2Ops[K1, K2, T] = new Rollup2Ops[K1, K2, T](this, c1, c2)

  /**
    * Create a multi-dimensional rollup for the current [[TypedDataset]] using the specified columns,
    * so we can run aggregation on them.
    * See [[frameless.functions.AggregateFunctions]] for all the available aggregate functions.
    *
    * {{{
    *   case class MyClass(a: Int, b: Int, c: Int)
    *   val ds: TypedDataset[MyClass]
    *
    *   val cubeDataset: TypedDataset[(Option[A], Option[B], Long)] =
    *     ds.rollupMany(ds('a), ds('b)).agg(count[MyClass]())
    *
    *   // original dataset:
    *     a       b     c
    *    10      20     1
    *    15      25     2
    *
    *   // after aggregation:
    *     _1      _2   _3
    *     15    null    1
    *     15      25    1
    *   null    null    2
    *     10    null    1
    *     10      20    1
    *
    * }}}
    *
    * Differs from `Dataset#rollup` by wrapping values into `Option` instead of returning `null`.
    *
    * apache/spark
    */
  object rollupMany extends ProductArgs {
    def applyProduct[TK <: HList, K <: HList, KT](groupedBy: TK)
      (implicit
        i0: ColumnTypes.Aux[T, TK, K],
        i1: Tupler.Aux[K, KT],
        i2: ToTraversable.Aux[TK, List, UntypedExpression[T]]
      ): RollupManyOps[T, TK, K, KT] = new RollupManyOps[T, TK, K, KT](self, groupedBy)
  }

  /** Computes the cartesian project of `this` `Dataset` with the `other` `Dataset` */
  def joinCross[U](other: TypedDataset[U])
    (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
      new TypedDataset(self.dataset.joinWith(other.dataset, new Column(Literal(true)), "cross"))

  /** Computes the full outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinFull[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(Option[T], Option[U])]): TypedDataset[(Option[T], Option[U])] =
    new TypedDataset(self.dataset.joinWith(other.dataset, condition.untyped, "full")
      .as[(Option[T], Option[U])](TypedExpressionEncoder[(Option[T], Option[U])]))

  /** Computes the inner join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinInner[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] = {
      import FramelessInternals._
      val leftPlan = logicalPlan(dataset)
      val rightPlan = logicalPlan(other.dataset)
      val join = disambiguate(Join(leftPlan, rightPlan, Inner, Some(condition.expr), JoinHint.NONE))
      val joinedPlan = joinPlan(dataset, join, leftPlan, rightPlan)
      val joinedDs = mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, U)])
      TypedDataset.create[(T, U)](joinedDs)
    }

  /** Computes the left outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeft[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(T, Option[U])]): TypedDataset[(T, Option[U])] =
      new TypedDataset(self.dataset.joinWith(other.dataset, condition.untyped, "left_outer")
        .as[(T, Option[U])](TypedExpressionEncoder[(T, Option[U])]))

  /** Computes the left semi join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeftSemi[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean]): TypedDataset[T] =
    new TypedDataset(self.dataset.join(other.dataset, condition.untyped, "leftsemi")
      .as[T](TypedExpressionEncoder(encoder)))

  /** Computes the left anti join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeftAnti[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean]): TypedDataset[T] =
    new TypedDataset(self.dataset.join(other.dataset, condition.untyped, "leftanti")
      .as[T](TypedExpressionEncoder(encoder)))

  /** Computes the right outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinRight[U](other: TypedDataset[U])(condition: TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(Option[T], U)]): TypedDataset[(Option[T], U)] =
    new TypedDataset(self.dataset.joinWith(other.dataset, condition.untyped, "right_outer")
      .as[(Option[T], U)](TypedExpressionEncoder[(Option[T], U)]))

  private def disambiguate(join: Join): Join = {
    val plan = FramelessInternals.ofRows(dataset.sparkSession, join).queryExecution.analyzed.asInstanceOf[Join]
    val disambiguated = plan.condition.map(_.transform {
      case FramelessInternals.DisambiguateLeft(tagged: AttributeReference) =>
        val leftDs = FramelessInternals.ofRows(spark, plan.left)
        FramelessInternals.resolveExpr(leftDs, Seq(tagged.name))

      case FramelessInternals.DisambiguateRight(tagged: AttributeReference) =>
        val rightDs = FramelessInternals.ofRows(spark, plan.right)
        FramelessInternals.resolveExpr(rightDs, Seq(tagged.name))

      case x => x
    })
    plan.copy(condition = disambiguated)
  }

  /** Takes a function from A => R and converts it to a UDF for TypedColumn[T, A] => TypedColumn[T, R].
    */
  def makeUDF[A: TypedEncoder, R: TypedEncoder](f: A => R):
  TypedColumn[T, A] => TypedColumn[T, R] = functions.udf(f)

  /** Takes a function from (A1, A2) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, R: TypedEncoder](f: (A1, A2) => R):
  (TypedColumn[T, A1], TypedColumn[T, A2]) => TypedColumn[T, R] = functions.udf(f)

  /** Takes a function from (A1, A2, A3) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3) => R):
  (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3]) => TypedColumn[T, R] = functions.udf(f)

  /** Takes a function from (A1, A2, A3, A4) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, A4: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3, A4) => R):
  (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R] = functions.udf(f)

  /** Takes a function from (A1, A2, A3, A4, A5) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, A4: TypedEncoder, A5: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3, A4, A5) => R):
  (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R] = functions.udf(f)

  /** Type-safe projection from type T to Tuple1[A]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A](
    ca: TypedColumn[T, A]
  ): TypedDataset[A] = {
    implicit val ea = ca.uencoder

    val tuple1: TypedDataset[Tuple1[A]] = selectMany(ca)

    // now we need to unpack `Tuple1[A]` to `A`

    TypedEncoder[A].catalystRepr match {
      case StructType(_) =>
        // if column is struct, we use all its fields
        val df = tuple1
          .dataset
          .selectExpr("_1.*")
          .as[A](TypedExpressionEncoder[A])

        TypedDataset.create(df)
      case other =>
        // for primitive types `Tuple1[A]` has the same schema as `A`
        TypedDataset.create(tuple1.dataset.as[A](TypedExpressionEncoder[A]))
    }
  }

  /** Type-safe projection from type T to Tuple2[A,B]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B]
  ): TypedDataset[(A, B)] = {
    implicit val (ea, eb) = (ca.uencoder, cb.uencoder)
    selectMany(ca, cb)
  }

  /** Type-safe projection from type T to Tuple3[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C]
  ): TypedDataset[(A, B, C)] = {
    implicit val (ea, eb, ec) = (ca.uencoder, cb.uencoder, cc.uencoder)
    selectMany(ca, cb, cc)
  }

  /** Type-safe projection from type T to Tuple4[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D]
  ): TypedDataset[(A, B, C, D)] = {
    implicit val (ea, eb, ec, ed) = (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder)
    selectMany(ca, cb, cc, cd)
  }

  /** Type-safe projection from type T to Tuple5[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E]
  ): TypedDataset[(A, B, C, D, E)] = {
    implicit val (ea, eb, ec, ed, ee) =
      (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder)

    selectMany(ca, cb, cc, cd, ce)
  }

  /** Type-safe projection from type T to Tuple6[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E, F](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E],
    cf: TypedColumn[T, F]
  ): TypedDataset[(A, B, C, D, E, F)] = {
    implicit val (ea, eb, ec, ed, ee, ef) =
      (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder, cf.uencoder)

    selectMany(ca, cb, cc, cd, ce, cf)
  }

  /** Type-safe projection from type T to Tuple7[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E, F, G](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E],
    cf: TypedColumn[T, F],
    cg: TypedColumn[T, G]
  ): TypedDataset[(A, B, C, D, E, F, G)] = {
    implicit val (ea, eb, ec, ed, ee, ef, eg) =
      (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder, cf.uencoder, cg.uencoder)

    selectMany(ca, cb, cc, cd, ce, cf, cg)
  }

  /** Type-safe projection from type T to Tuple8[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E, F, G, H](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E],
    cf: TypedColumn[T, F],
    cg: TypedColumn[T, G],
    ch: TypedColumn[T, H]
  ): TypedDataset[(A, B, C, D, E, F, G, H)] = {
    implicit val (ea, eb, ec, ed, ee, ef, eg, eh) =
      (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder, cf.uencoder, cg.uencoder, ch.uencoder)

    selectMany(ca, cb, cc, cd, ce, cf, cg, ch)
  }

  /** Type-safe projection from type T to Tuple9[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E, F, G, H, I](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E],
    cf: TypedColumn[T, F],
    cg: TypedColumn[T, G],
    ch: TypedColumn[T, H],
    ci: TypedColumn[T, I]
  ): TypedDataset[(A, B, C, D, E, F, G, H, I)] = {
    implicit val (ea, eb, ec, ed, ee, ef, eg, eh, ei) =
       (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder, cf.uencoder, cg.uencoder, ch.uencoder, ci.uencoder)

    selectMany(ca, cb, cc, cd, ce, cf, cg, ch, ci)
  }

  /** Type-safe projection from type T to Tuple10[A,B,...]
    * {{{
    *   d.select( d('a), d('a)+d('b), ... )
    * }}}
    */
  def select[A, B, C, D, E, F, G, H, I, J](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C],
    cd: TypedColumn[T, D],
    ce: TypedColumn[T, E],
    cf: TypedColumn[T, F],
    cg: TypedColumn[T, G],
    ch: TypedColumn[T, H],
    ci: TypedColumn[T, I],
    cj: TypedColumn[T, J]
  ): TypedDataset[(A, B, C, D, E, F, G, H, I, J)] = {
    implicit val (ea, eb, ec, ed, ee, ef, eg, eh, ei, ej) =
      (ca.uencoder, cb.uencoder, cc.uencoder, cd.uencoder, ce.uencoder, cf.uencoder, cg.uencoder, ch.uencoder, ci.uencoder, cj.uencoder)
    selectMany(ca, cb, cc, cd, ce, cf, cg, ch, ci, cj)
  }

  object selectMany extends ProductArgs {
    def applyProduct[U <: HList, Out0 <: HList, Out](columns: U)
      (implicit
        i0: ColumnTypes.Aux[T, U, Out0],
        i1: ToTraversable.Aux[U, List, UntypedExpression[T]],
        i2: Tupler.Aux[Out0, Out],
        i3: TypedEncoder[Out]
      ): TypedDataset[Out] = {
        val base = dataset.toDF()
          .select(columns.toList[UntypedExpression[T]].map(c => new Column(c.expr)):_*)
        val selected = base.as[Out](TypedExpressionEncoder[Out])

        TypedDataset.create[Out](selected)
      }
  }

  /** Sort each partition in the dataset using the columns selected. */
  def sortWithinPartitions[A: CatalystOrdered](ca: SortedTypedColumn[T, A]): TypedDataset[T] =
    sortWithinPartitionsMany(ca)

  /** Sort each partition in the dataset using the columns selected. */
  def sortWithinPartitions[A: CatalystOrdered, B: CatalystOrdered](
    ca: SortedTypedColumn[T, A],
    cb: SortedTypedColumn[T, B]
  ): TypedDataset[T] = sortWithinPartitionsMany(ca, cb)

  /** Sort each partition in the dataset using the columns selected. */
  def sortWithinPartitions[A: CatalystOrdered, B: CatalystOrdered, C: CatalystOrdered](
    ca: SortedTypedColumn[T, A],
    cb: SortedTypedColumn[T, B],
    cc: SortedTypedColumn[T, C]
  ): TypedDataset[T] = sortWithinPartitionsMany(ca, cb, cc)

  /** Sort each partition in the dataset by the given column expressions
    * Default sort order is ascending.
    * {{{
    *   d.sortWithinPartitionsMany(d('a), d('b).desc, d('c).asc)
    * }}}
    */
  object sortWithinPartitionsMany extends ProductArgs {
    def applyProduct[U <: HList, O <: HList](columns: U)
      (implicit
        i0: Mapper.Aux[SortedTypedColumn.defaultAscendingPoly.type, U, O],
        i1: ToTraversable.Aux[O, List, SortedTypedColumn[T, _]]
      ): TypedDataset[T] = {
      val sorted = dataset.toDF()
        .sortWithinPartitions(i0(columns).toList[SortedTypedColumn[T, _]].map(_.untyped):_*)
        .as[T](TypedExpressionEncoder[T])

      TypedDataset.create[T](sorted)
    }
  }

  /** Orders the TypedDataset using the column selected. */
  def orderBy[A: CatalystOrdered](ca: SortedTypedColumn[T, A]): TypedDataset[T] =
    orderByMany(ca)

  /** Orders the TypedDataset using the columns selected. */
  def orderBy[A: CatalystOrdered, B: CatalystOrdered](
    ca: SortedTypedColumn[T, A],
    cb: SortedTypedColumn[T, B]
  ): TypedDataset[T] = orderByMany(ca, cb)

 /** Orders the TypedDataset using the columns selected. */
 def orderBy[A: CatalystOrdered, B: CatalystOrdered, C: CatalystOrdered](
   ca: SortedTypedColumn[T, A],
   cb: SortedTypedColumn[T, B],
   cc: SortedTypedColumn[T, C]
 ): TypedDataset[T] = orderByMany(ca, cb, cc)

  /** Sort the dataset by any number of column expressions.
    * Default sort order is ascending.
    * {{{
    *   d.orderByMany(d('a), d('b).desc, d('c).asc)
    * }}}
    */
  object orderByMany extends ProductArgs {
    def applyProduct[U <: HList, O <: HList](columns: U)
      (implicit
        i0: Mapper.Aux[SortedTypedColumn.defaultAscendingPoly.type, U, O],
        i1: ToTraversable.Aux[O, List, SortedTypedColumn[T, _]]
      ): TypedDataset[T] = {
      val sorted = dataset.toDF()
        .orderBy(i0(columns).toList[SortedTypedColumn[T, _]].map(_.untyped):_*)
        .as[T](TypedExpressionEncoder[T])

      TypedDataset.create[T](sorted)
    }
  }

  /** Returns a new Dataset as a tuple with the specified
    * column dropped.
    * Does not allow for dropping from a single column TypedDataset
    *
    * {{{
    *   val d: TypedDataset[Foo(a: String, b: Int...)] = ???
    *   val result = TypedDataset[(Int, ...)] = d.drop('a)
    * }}}
    * @param column column to drop specified as a Symbol
    * @param i0 LabelledGeneric derived for T
    * @param i1 Remover derived for TRep and column
    * @param i2 values of T with column removed
    * @param i3 tupler of values
    * @param i4 evidence of encoder of the tupled values
    * @tparam Out Tupled return type
    * @tparam TRep shapeless' record representation of T
    * @tparam Removed record of T with column removed
    * @tparam ValuesFromRemoved values of T with column removed as an HList
    * @tparam V value type of column in T
    * @return
    */
  def dropTupled[Out, TRep <: HList, Removed <: HList, ValuesFromRemoved <: HList, V]
    (column: Witness.Lt[Symbol])
    (implicit
      i0: LabelledGeneric.Aux[T, TRep],
      i1: Remover.Aux[TRep, column.T, (V, Removed)],
      i2: Values.Aux[Removed, ValuesFromRemoved],
      i3: Tupler.Aux[ValuesFromRemoved, Out],
      i4: TypedEncoder[Out]
    ): TypedDataset[Out] = {
      val dropped = dataset
        .toDF()
        .drop(column.value.name)
        .as[Out](TypedExpressionEncoder[Out])

      TypedDataset.create[Out](dropped)
    }

  /**
    * Drops columns as necessary to return `U`
    *
    * @example
    * {{{
    *   case class X(i: Int, j: Int, k: Boolean)
    *   case class Y(i: Int, k: Boolean)
    *   val f: TypedDataset[X] = ???
    *   val fNew: TypedDataset[Y] = f.drop[Y]
    * }}}
    *
    * @tparam U the output type
    *
    * @see [[frameless.TypedDataset#project]]
    */
  def drop[U](implicit projector: SmartProject[T,U]): TypedDataset[U] = project[U]

  /** Prepends a new column to the Dataset.
    *
    * {{{
    *   case class X(i: Int, j: Int)
    *   val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    *   val fNew: TypedDataset[(Int,Int,Boolean)] = f.withColumnTupled(f('j) === 10)
    * }}}
    */
  def withColumnTupled[A: TypedEncoder, H <: HList, FH <: HList, Out]
    (ca: TypedColumn[T, A])
    (implicit
      i0: Generic.Aux[T, H],
      i1: Prepend.Aux[H, A :: HNil, FH],
      i2: Tupler.Aux[FH, Out],
      i3: TypedEncoder[Out]
    ): TypedDataset[Out] = {
      // Giving a random name to the new column (the proper name will be given by the Tuple-based encoder)
      val selected = dataset.toDF().withColumn("I1X3T9CU1OP0128JYIO76TYZZA3AXHQ18RMI", ca.untyped)
        .as[Out](TypedExpressionEncoder[Out])

      TypedDataset.create[Out](selected)
  }

  /** Returns a new [[frameless.TypedDataset]] with the specified column updated with a new value
    * {{{
    *   case class X(i: Int, j: Int)
    *   val f: TypedDataset[X] = TypedDataset.create(X(1,10) :: Nil)
    *   val fNew: TypedDataset[X] = f.withColumn('j, f('i)) // results in X(1, 1) :: Nil
    * }}}
    * @param column column given as a symbol to replace
    * @param replacement column to replace the value with
    * @param i0 Evidence that a column with the correct type and name exists
    */
  def withColumnReplaced[A, Tw](
    column: Witness.Lt[Symbol],
    replacement: TypedColumn[T, A]
  )(implicit
    i0: SchemaWrapper.Aux[T, Tw],
    i1: TypedColumn.Exists[Tw, column.T, A]
  ): TypedDataset[T] = {
    val updated = dataset.toDF().withColumn(column.value.name, replacement.untyped)
      .as[T](TypedExpressionEncoder[T])

    TypedDataset.create[T](updated)
  }

  /** Adds a column to a Dataset so long as the specified output type, `U`, has
    * an extra column from `T` that has type `A`.
    *
    * @example
    * {{{
    *   case class X(i: Int, j: Int)
    *   case class Y(i: Int, j: Int, k: Boolean)
    *   val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    *   val fNew: TypedDataset[Y] = f.withColumn[Y](f('j) === 10)
    * }}}
    * @param ca The typed column to add
    * @param i0 TypeEncoder for output type U
    * @param i1 TypeEncoder for added column type A
    * @param i2 the LabelledGeneric derived for T
    * @param i3 the LabelledGeneric derived for U
    * @param i4 proof no fields have been removed
    * @param i5 diff from T to U
    * @param i6 keys from newFields
    * @param i7 the one and only new key
    * @param i8 the one and only new field enforcing the type of A exists
    * @param i9 the keys of U
    * @param iA allows for traversing the keys of U
    * @tparam U the output type
    * @tparam A The added column type
    * @tparam TRep shapeless' record representation of T
    * @tparam URep shapeless' record representation of U
    * @tparam UKeys the keys of U as an HList
    * @tparam NewFields the added fields to T to get U
    * @tparam NewKeys the keys of NewFields as an HList
    * @tparam NewKey the first, and only, key in NewKey
    *
    * @see [[frameless.TypedDataset.WithColumnApply#apply]]
    */
  def withColumn[U] = new WithColumnApply[U]

  class WithColumnApply[U] {
    def apply[A, TRep <: HList, URep <: HList, UKeys <: HList, NewFields <: HList, NewKeys <: HList, NewKey <: Symbol]
    (ca: TypedColumn[T, A])
    (implicit
      i0: TypedEncoder[U],
      i1: TypedEncoder[A],
      i2: LabelledGeneric.Aux[T, TRep],
      i3: LabelledGeneric.Aux[U, URep],
      i4: Diff.Aux[TRep, URep, HNil],
      i5: Diff.Aux[URep, TRep, NewFields],
      i6: Keys.Aux[NewFields, NewKeys],
      i7: IsHCons.Aux[NewKeys, NewKey, HNil],
      i8: IsHCons.Aux[NewFields, FieldType[NewKey, A], HNil],
      i9: Keys.Aux[URep, UKeys],
      iA: ToTraversable.Aux[UKeys, Seq, Symbol]
    ): TypedDataset[U] = {
      val newColumnName =
        i7.head(i6()).name

      val dfWithNewColumn = dataset
        .toDF()
        .withColumn(newColumnName, ca.untyped)

      val newColumns = i9.apply.to[Seq].map(_.name).map(dfWithNewColumn.col)

      val selected = dfWithNewColumn
        .select(newColumns: _*)
        .as[U](TypedExpressionEncoder[U])

      TypedDataset.create[U](selected)
    }
  }

  /**
    * Explodes a single column at a time. It only compiles if the type of column supports this operation.
    *
    * @example
    *
    * {{{
    *   case class X(i: Int, j: Array[Int])
    *   case class Y(i: Int, j: Int)
    *
    *   val f: TypedDataset[X] = ???
    *   val fNew: TypedDataset[Y] = f.explode('j).as[Y]
    * }}}
    * @param column the column we wish to explode
    */
  def explode[A, Tw, TRep <: HList, V[_], OutMod <: HList, OutModValues <: HList, Out]
  (column: Witness.Lt[Symbol])
  (implicit
   i0: SchemaWrapper.Aux[T, Tw],
   i1: TypedColumn.Exists[Tw, column.T, V[A]],
   i2: TypedEncoder[A],
   i3: CatalystExplodableCollection[V],
   i4: LabelledGeneric.Aux[T, TRep],
   i5: Modifier.Aux[TRep, column.T, V[A], A, OutMod],
   i6: Values.Aux[OutMod, OutModValues],
   i8: Tupler.Aux[OutModValues, Out],
   i7: TypedEncoder[Out]
  ): TypedDataset[Out] = {
    val df = dataset.toDF()
    import org.apache.spark.sql.functions.{explode => sparkExplode}

    val trans =
      df.withColumn(column.value.name,
        sparkExplode(df(column.value.name))).as[Out](TypedExpressionEncoder[Out])
    TypedDataset.create[Out](trans)
  }

  /**
    * Flattens a column of type Option[A]. Compiles only if the selected column is of type Option[A].
    *
    *
    * @example
    *
    * {{{
    *   case class X(i: Int, j: Option[Int])
    *   case class Y(i: Int, j: Int)
    *
    *   val f: TypedDataset[X] = ???
    *   val fNew: TypedDataset[Y] = f.flattenOption('j).as[Y]
    * }}}
    *
    * @param column the column we wish to flatten
    */
  def flattenOption[A, TRep <: HList, V[_], OutMod <: HList, OutModValues <: HList, Out]
  (column: Witness.Lt[Symbol])
  (implicit
   i0: TypedColumn.Exists[T, column.T, V[A]],
   i1: TypedEncoder[A],
   i2: V[A] =:= Option[A],
   i3: LabelledGeneric.Aux[T, TRep],
   i4: Modifier.Aux[TRep, column.T, V[A], A, OutMod],
   i5: Values.Aux[OutMod, OutModValues],
   i6: Tupler.Aux[OutModValues, Out],
   i7: TypedEncoder[Out]
  ): TypedDataset[Out] = {
    val df = dataset.toDF()
    val trans = 
      df.filter(df(column.value.name).isNotNull).as[Out](TypedExpressionEncoder[Out])
    TypedDataset.create[Out](trans)
  }
}

object TypedDataset {
  def create[A](data: Seq[A])
    (implicit
      encoder: TypedEncoder[A],
      sqlContext: SparkSession
    ): TypedDataset[A] = {
      val dataset = sqlContext.createDataset(data)(TypedExpressionEncoder[A])
      TypedDataset.create[A](dataset)
    }

  def create[A](data: RDD[A])
    (implicit
      encoder: TypedEncoder[A],
      sqlContext: SparkSession
    ): TypedDataset[A] = {
      val dataset = sqlContext.createDataset(data)(TypedExpressionEncoder[A])
      TypedDataset.create[A](dataset)
    }

  def create[A: TypedEncoder](dataset: Dataset[A]): TypedDataset[A] = createUnsafe(dataset.toDF())

  /**
    * Creates a [[frameless.TypedDataset]] from a Spark [[org.apache.spark.sql.DataFrame]].
    * Note that the names and types need to align!
    *
    * This is an unsafe operation: If the schemas do not align,
    * the error will be captured at runtime (not during compilation).
    */
  def createUnsafe[A: TypedEncoder](df: DataFrame): TypedDataset[A] = {
    val e = TypedEncoder[A]
    val output: Seq[Attribute] = df.queryExecution.analyzed.output

    val targetFields = TypedExpressionEncoder.targetStructType(e)
    val targetColNames: Seq[String] = targetFields.map(_.name)

    if (output.size != targetFields.size) {
      throw new IllegalStateException(
        s"Unsupported creation of TypedDataset with ${targetFields.size} column(s) " +
          s"from a DataFrame with ${output.size} columns. " +
          "Try to `select()` the proper columns in the right order before calling `create()`.")
    }

    // Adapt names if they are not the same (note: types still might not match)
    val shouldReshape = output.zip(targetColNames).exists {
      case (expr, colName) => expr.name != colName
    }
    val canSelect = targetColNames.toSet.subsetOf(output.map(_.name).toSet)

    val reshaped = if (shouldReshape && canSelect) {
      df.select(targetColNames.head, targetColNames.tail:_*)
    } else if (shouldReshape) {
      df.toDF(targetColNames: _*)
    } else {
      df
    }

    new TypedDataset[A](reshaped.as[A](TypedExpressionEncoder[A]))
  }

  /** Prefer `TypedDataset.create` over `TypedDataset.unsafeCreate` unless you
    * know what you are doing. */
  @deprecated("Prefer TypedDataset.create over TypedDataset.unsafeCreate", "0.3.0")
  def unsafeCreate[A: TypedEncoder](dataset: Dataset[A]): TypedDataset[A] = {
    new TypedDataset[A](dataset)
  }
}
