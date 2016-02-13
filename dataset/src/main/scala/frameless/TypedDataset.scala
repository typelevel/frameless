package frameless

import frameless.ops._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct, EqualTo}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.{FramelessInternals, Column, SQLContext, Dataset}
import shapeless.ops.hlist.{Tupler, ToTraversable}
import shapeless._

class TypedDataset[T](
  val dataset: Dataset[T]
)(implicit val encoder: TypedEncoder[T]) { self =>

  def as[U]()(implicit as: As[T, U]): TypedDataset[U] = {
    implicit val uencoder = as.encoder
    new TypedDataset(dataset.as[U](TypedExpressionEncoder[U]))
  }

  def coalesce(numPartitions: Int): TypedDataset[T] =
    new TypedDataset(dataset.coalesce(numPartitions))

  /**
    * Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    *   tf.col('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def col[A](column: Witness.Lt[Symbol])(
    implicit
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]
  ): TypedColumn[T, A] = {
    val colExpr = FramelessInternals.resolveExpr(dataset, column.value.name :: Nil)
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

  /**
    * Job returns an array that contains all the elements in this [[TypedDataset]].
    *
    * Running job requires moving all the data into the application's driver process, and
    * doing so on a very large [[TypedDataset]] can crash the driver process with OutOfMemoryError.
    */
  def collect(): Job[Array[T]] = Job(dataset.collect())(dataset.sqlContext.sparkContext)

  /** Returns a new [[frameless.TypedDataset]] that only contains elements where `column` is `true`. */
  def filter(column: TypedColumn[T, Boolean]): TypedDataset[T] = {
    val filtered = dataset.toDF()
      .filter(new Column(column.expr))
      .as[T](TypedExpressionEncoder[T])

    new TypedDataset[T](filtered)
  }

  def groupBy[K1](
    c1: TypedColumn[T, K1]
  ): GroupedBy1Ops[K1, T] = new GroupedBy1Ops[K1, T](this, c1)

  def groupBy[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): GroupedBy2Ops[K1, K2, T] = new GroupedBy2Ops[K1, K2, T](this, c1, c2)

  object groupByMany extends ProductArgs {
    def applyProduct[K <: HList, Out0 <: HList, Out](groupedBy: K)(
      implicit
      ct: ColumnTypes.Aux[T, K, Out0],
      toTraversable: ToTraversable.Aux[K, List, UntypedColumn[T]]
    ): GroupedByManyOps[T, K, Out0] = new GroupedByManyOps[T, K, Out0](self, groupedBy)
  }

  def join[A: TypedEncoder, B](
    right: TypedDataset[A],
    leftCol: TypedColumn[T, B],
    rightCol: TypedColumn[A, B]
  )(implicit e: TypedEncoder[(T, A)]): TypedDataset[(T, A)] = {
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

    new TypedDataset[(T, A)](joinedDs)
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

    new TypedDataset[(T, Option[A])](joinedDs)
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
      toTraversable: ToTraversable.Aux[U, List, UntypedColumn[T]],
      tupler: Tupler.Aux[Out0, Out],
      encoder: TypedEncoder[Out]
    ): TypedDataset[Out] = {
      val selected = dataset.toDF()
        .select(toTraversable(columns).map(c => new Column(c.expr)): _*)
        .as[Out](TypedExpressionEncoder[Out])

      new TypedDataset[Out](selected)
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
    new TypedDataset[A](dataset)
  }

  def create[A](data: RDD[A])(
    implicit
    encoder: TypedEncoder[A],
    sqlContext: SQLContext
  ): TypedDataset[A] = {
    val dataset = sqlContext.createDataset(data)(TypedExpressionEncoder[A])
    new TypedDataset[A](dataset)
  }
}
