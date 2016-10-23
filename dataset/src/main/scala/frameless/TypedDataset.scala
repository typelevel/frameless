package frameless

import frameless.ops._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct, EqualTo}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.{Column, Dataset, FramelessInternals, SQLContext}
import shapeless.ops.hlist.{ToTraversable, Tupler}
import shapeless._

/** [[TypedDataset]] is a safer interface for working with `Dataset`.
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
class TypedDataset[T] protected[frameless](val dataset: Dataset[T])(implicit val encoder: TypedEncoder[T])
    extends TypedDatasetForwarded[T] { self =>

  private implicit val sparkContext = dataset.sqlContext.sparkContext

  /** Returns a new [[TypedDataset]] where each record has been mapped on to the specified type. */
  def as[U]()(implicit as: As[T, U]): TypedDataset[U] = {
    implicit val uencoder = as.encoder
    TypedDataset.create(dataset.as[U](TypedExpressionEncoder[U]))
  }

  /** Returns the number of elements in the [[TypedDataset]].
    *
    * Differs from `Dataset#count` by wrapping it's result into a [[Job]].
    */
  def count(): Job[Long] =
    Job(dataset.count)

  /** Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    * tf.col('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def col[A](column: Witness.Lt[Symbol])(
    implicit
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]
  ): TypedColumn[T, A] = {
    val colExpr = dataset.col(column.value.name).as[A](TypedExpressionEncoder[A])
    new TypedColumn[T, A](colExpr)
  }

  object colMany extends SingletonProductArgs {
    def applyProduct[U <: HList, Out](columns: U)(
      implicit
      existsAll: TypedColumn.ExistsMany[T, U, Out],
      encoder: TypedEncoder[Out],
      toTraversable: ToTraversable.Aux[U, List, Symbol]
    ): TypedColumn[T, Out] = {
      val names = toTraversable(columns).map(_.name)
      val colExpr = FramelessInternals.resolveExpr(dataset, names)

      new TypedColumn[T, Out](colExpr)
    }
  }

  /** Returns a `Seq` that contains all the elements in this [[TypedDataset]].
    *
    * Running this [[Job]] requires moving all the data into the application's driver process, and
    * doing so on a very large [[TypedDataset]] can crash the driver process with OutOfMemoryError.
    *
    * Differs from `Dataset#collect` by wrapping it's result into a [[Job]].
    */
  def collect(): Job[Seq[T]] =
    Job(dataset.collect())

  /** Optionally returns the first element in this [[TypedDataset]].
    *
    * Differs from `Dataset#first` by wrapping it's result into an `Option` and a [[Job]].
    */
  def firstOption(): Job[Option[T]] =
    Job {
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
    * Differs from `Dataset#take` by wrapping it's result into a [[Job]].
    *
    * apache/spark
    */
  def take(num: Int): Job[Seq[T]] =
    Job(dataset.take(num))

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
    * Differs from `Dataset#show` by wrapping it's result into a [[Job]].
    *
    * apache/spark
    */
  def show(numRows: Int = 20, truncate: Boolean = true): Job[Unit] =
    Job(dataset.show(numRows, truncate))

  /** Returns a new [[frameless.TypedDataset]] that only contains elements where `column` is `true`.
    *
    * Differs from `TypedDatasetForward#filter` by taking a `TypedColumn[T, Boolean]` instead of a
    * `T => Boolean`. Using a column expression instead of a regular function save one Spark → Scala
    * deserialization which leads to better preformances.
    */
  def filter(column: TypedColumn[T, Boolean]): TypedDataset[T] = {
    val filtered = dataset.toDF()
      .filter(new Column(column.expr))
      .as[T](TypedExpressionEncoder[T])

    TypedDataset.create[T](filtered)
  }

  /** Runs `func` on each element of this [[TypedDataset]].
    *
    * Differs from `Dataset#foreach` by wrapping it's result into a [[Job]].
    */
  def foreach(func: T => Unit): Job[Unit] =
    Job(dataset.foreach(func))

  /** Runs `func` on each partition of this [[TypedDataset]].
    *
    * Differs from `Dataset#foreachPartition` by wrapping it's result into a [[Job]].
    */
  def foreachPartition(func: Iterator[T] => Unit): Job[Unit] =
    Job(dataset.foreachPartition(func))

  /** Optionally reduces the elements of this [[TypedDataset]] using the specified binary function. The given
    * `func` must be commutative and associative or the result may be non-deterministic.
    *
    * Differs from `Dataset#reduce` by wrapping it's result into an `Option` and a [[Job]].
    */
  def reduceOption(func: (T, T) => T): Job[Option[T]] =
    Job {
      try {
        Option(dataset.reduce(func))
      } catch {
        case e: UnsupportedOperationException => None
      }
    }

  def groupBy[K1](
    c1: TypedColumn[T, K1]
  ): GroupedBy1Ops[K1, T] = new GroupedBy1Ops[K1, T](this, c1)

  def groupBy[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): GroupedBy2Ops[K1, K2, T] = new GroupedBy2Ops[K1, K2, T](this, c1, c2)

  object groupByMany extends ProductArgs {
    def applyProduct[TK <: HList, K <: HList, KT](groupedBy: TK)(
      implicit
      ct: ColumnTypes.Aux[T, TK, K],
      tupler: Tupler.Aux[K, KT],
      toTraversable: ToTraversable.Aux[TK, List, UntypedExpression[T]]
    ): GroupedByManyOps[T, TK, K, KT] = new GroupedByManyOps[T, TK, K, KT](self, groupedBy)
  }

  def join[A, B](
    right: TypedDataset[A],
    leftCol: TypedColumn[T, B],
    rightCol: TypedColumn[A, B]
  ): TypedDataset[(T, A)] = {
    implicit def re = right.encoder

    val leftPlan = FramelessInternals.logicalPlan(dataset)
    val rightPlan = FramelessInternals.logicalPlan(right.dataset)
    val condition = EqualTo(leftCol.expr, rightCol.expr)

    val joined = FramelessInternals.executePlan(dataset, Join(leftPlan, rightPlan, Inner, Some(condition)))
    val leftOutput = joined.analyzed.output.take(leftPlan.output.length)
    val rightOutput = joined.analyzed.output.takeRight(rightPlan.output.length)

    val joinedPlan = Project(List(
      Alias(CreateStruct(leftOutput), "_1")(),
      Alias(CreateStruct(rightOutput), "_2")()
    ), joined.analyzed)

    val joinedDs = FramelessInternals.mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, A)])

    TypedDataset.create[(T, A)](joinedDs)
  }

  def joinLeft[A: TypedEncoder, B](
    right: TypedDataset[A],
    leftCol: TypedColumn[T, B],
    rightCol: TypedColumn[A, B]
  )(implicit e: TypedEncoder[(T, Option[A])]): TypedDataset[(T, Option[A])] = {
    val leftPlan = FramelessInternals.logicalPlan(dataset)
    val rightPlan = FramelessInternals.logicalPlan(right.dataset)
    val condition = EqualTo(leftCol.expr, rightCol.expr)

    val joined = FramelessInternals.executePlan(dataset, Join(leftPlan, rightPlan, LeftOuter, Some(condition)))
    val leftOutput = joined.analyzed.output.take(leftPlan.output.length)
    val rightOutput = joined.analyzed.output.takeRight(rightPlan.output.length)

    val joinedPlan = Project(List(
      Alias(CreateStruct(leftOutput), "_1")(),
      Alias(CreateStruct(rightOutput), "_2")()
    ), joined.analyzed)

    val joinedDs = FramelessInternals.mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, Option[A])])

    TypedDataset.create[(T, Option[A])](joinedDs)
  }

  def select[A: TypedEncoder](
    ca: TypedColumn[T, A]
  ): TypedDataset[A] = selectMany(ca).as[A]() // TODO fix selectMany for a single parameter

  def select[A: TypedEncoder, B: TypedEncoder](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B]
  ): TypedDataset[(A, B)] = selectMany(ca, cb)

  def select[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
    ca: TypedColumn[T, A],
    cb: TypedColumn[T, B],
    cc: TypedColumn[T, C]
  ): TypedDataset[(A, B, C)] = selectMany(ca, cb, cc)

  object selectMany extends ProductArgs {
    def applyProduct[U <: HList, Out0 <: HList, Out](columns: U)(
      implicit
      ct: ColumnTypes.Aux[T, U, Out0],
      toTraversable: ToTraversable.Aux[U, List, UntypedExpression[T]],
      tupler: Tupler.Aux[Out0, Out],
      encoder: TypedEncoder[Out]
    ): TypedDataset[Out] = {
      val selected = dataset.toDF()
        .select(toTraversable(columns).map(c => new Column(c.expr)): _*)
        .as[Out](TypedExpressionEncoder[Out])

      TypedDataset.create[Out](selected)
    }
  }
}

object TypedDataset {
  def create[A](data: Seq[A])(
    implicit
    encoder: TypedEncoder[A],
    sqlContext: SQLContext
  ): TypedDataset[A] = {
    val dataset = sqlContext.createDataset(data)(TypedExpressionEncoder[A])
    TypedDataset.create[A](dataset)
  }

  def create[A](data: RDD[A])(
    implicit
    encoder: TypedEncoder[A],
    sqlContext: SQLContext
  ): TypedDataset[A] = {
    val dataset = sqlContext.createDataset(data)(TypedExpressionEncoder[A])
    TypedDataset.create[A](dataset)
  }

  def create[A: TypedEncoder](dataset: Dataset[A]): TypedDataset[A] = {
    val e = TypedEncoder[A]
    val output = dataset.queryExecution.analyzed.output

    val fields = TypedExpressionEncoder.targetStructType(e)
    val colNames = fields.map(_.name)

    val shouldReshape = output.size != fields.size || output.zip(colNames).exists {
      case (expr, colName) => expr.name != colName
    }

    // NOTE: if output.size != fields.size Spark would generate Exception
    // It means that something has gone completely wrong and frameless schema has diverged from Spark one

    val reshaped =
      if (shouldReshape) dataset.toDF(colNames: _*)
      else dataset

    new TypedDataset[A](reshaped.as[A](TypedExpressionEncoder[A]))
  }

  /** Prefer `TypedDataset.create` over `TypedDataset.unsafeCreate` unless you
    * know what you are doing. */
  def unsafeCreate[A: TypedEncoder](dataset: Dataset[A]): TypedDataset[A] = {
    new TypedDataset[A](dataset)
  }
}
