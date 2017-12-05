package frameless

import frameless.ops._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateStruct, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql._
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Diff, IsHCons, Prepend, ToTraversable, Tupler}
import shapeless.ops.record.{Keys, Remover, Values}

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

        val cols = columns.toList[UntypedExpression[T]].map(c => new Column(c.expr))

        val selected = dataset.toDF()
          .agg(cols.head.alias("_1"), cols.tail: _*)
          .as[Out](TypedExpressionEncoder[Out])
          .filter("_1 is not null") // otherwise spark produces List(null) for empty datasets

        TypedDataset.create[Out](selected)
      }
  }

  /** Returns a new [[TypedDataset]] where each record has been mapped on to the specified type. */
  def as[U]()(implicit as: As[T, U]): TypedDataset[U] = {
    implicit val uencoder = as.encoder
    TypedDataset.create(dataset.as[U](TypedExpressionEncoder[U]))
  }

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

  /** Returns the number of elements in the [[TypedDataset]].
    *
    * Differs from `Dataset#count` by wrapping it's result into an effect-suspending `F[_]`.
    */
  def count[F[_]]()(implicit F: SparkDelay[F]): F[Long] =
    F.delay(dataset.count)

  /** Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    * tf('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def apply[A](column: Witness.Lt[Symbol])
    (implicit
      i0: TypedColumn.Exists[T, column.T, A],
      i1: TypedEncoder[A]
    ): TypedColumn[T, A] = col(column)

  /** Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    * tf.col('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def col[A](column: Witness.Lt[Symbol])
    (implicit
      i0: TypedColumn.Exists[T, column.T, A],
      i1: TypedEncoder[A]
    ): TypedColumn[T, A] = {
      val colExpr = dataset.col(column.value.name).as[A](TypedExpressionEncoder[A])
      new TypedColumn[T, A](colExpr)
    }

  object colMany extends SingletonProductArgs {
    def applyProduct[U <: HList, Out](columns: U)
      (implicit
        i0: TypedColumn.ExistsMany[T, U, Out],
        i1: TypedEncoder[Out],
        i2: ToTraversable.Aux[U, List, Symbol]
      ): TypedColumn[T, Out] = {
        val names = columns.toList[Symbol].map(_.name)
        val colExpr = FramelessInternals.resolveExpr(dataset, names)

          new TypedColumn[T, Out](colExpr)
      }
  }

  /** Returns a `Seq` that contains all the elements in this [[TypedDataset]].
    *
    * Running this operation requires moving all the data into the application's driver process, and
    * doing so on a very large [[TypedDataset]] can crash the driver process with OutOfMemoryError.
    *
    * Differs from `Dataset#collect` by wrapping it's result into an effect-suspending `F[_]`.
    */
  def collect[F[_]]()(implicit F: SparkDelay[F]): F[Seq[T]] =
    F.delay(dataset.collect())

  /** Optionally returns the first element in this [[TypedDataset]].
    *
    * Differs from `Dataset#first` by wrapping it's result into an `Option` and an effect-suspending `F[_]`.
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
    * Differs from `Dataset#take` by wrapping it's result into an effect-suspending `F[_]`.
    *
    * apache/spark
    */
  def take[F[_]](num: Int)(implicit F: SparkDelay[F]): F[Seq[T]] =
    F.delay(dataset.take(num))

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
    * Differs from `Dataset#show` by wrapping it's result into an effect-suspending `F[_]`.
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
    * Differs from `Dataset#foreach` by wrapping it's result into an effect-suspending `F[_]`.
    */
  def foreach[F[_]](func: T => Unit)(implicit F: SparkDelay[F]): F[Unit] =
    F.delay(dataset.foreach(func))

  /** Runs `func` on each partition of this [[TypedDataset]].
    *
    * Differs from `Dataset#foreachPartition` by wrapping it's result into an effect-suspending `F[_]`.
    */
  def foreachPartition[F[_]](func: Iterator[T] => Unit)(implicit F: SparkDelay[F]): F[Unit] =
    F.delay(dataset.foreachPartition(func))

  def groupBy[K1](
    c1: TypedColumn[T, K1]
  ): GroupedBy1Ops[K1, T] = new GroupedBy1Ops[K1, T](this, c1)

  def groupBy[K1, K2](
    c1: TypedColumn[T, K1],
    c2: TypedColumn[T, K2]
  ): GroupedBy2Ops[K1, K2, T] = new GroupedBy2Ops[K1, K2, T](this, c1, c2)

  object groupByMany extends ProductArgs {
    def applyProduct[TK <: HList, K <: HList, KT](groupedBy: TK)
      (implicit
        i0: ColumnTypes.Aux[T, TK, K],
        i1: Tupler.Aux[K, KT],
        i2: ToTraversable.Aux[TK, List, UntypedExpression[T]]
      ): GroupedByManyOps[T, TK, K, KT] = new GroupedByManyOps[T, TK, K, KT](self, groupedBy)
  }

  /** Fixes SPARK-6231, for more details see original code in [[Dataset#join]] **/
  private def resolveSelfJoin(join: Join): Join = {
    val plan = FramelessInternals.ofRows(dataset.sparkSession, join).queryExecution.analyzed.asInstanceOf[Join]
    val hasConflict = plan.left.output.intersect(plan.right.output).nonEmpty

    if (!hasConflict) {
      val selfJoinFix = spark.sqlContext.getConf("spark.sql.selfJoinAutoResolveAmbiguity", "true").toBoolean
      if (!selfJoinFix) throw new IllegalStateException("Frameless requires spark.sql.selfJoinAutoResolveAmbiguity to be true")

      val cond = plan.condition.map(_.transform {
        case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
          val leftDs = FramelessInternals.ofRows(spark, plan.left)
          val rightDs = FramelessInternals.ofRows(spark, plan.right)

          catalyst.expressions.EqualTo(
            FramelessInternals.resolveExpr(leftDs, Seq(a.name)),
            FramelessInternals.resolveExpr(rightDs, Seq(b.name))
          )
      })

      plan.copy(condition = cond)
    } else {
      join
    }
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

    val join = resolveSelfJoin(Join(leftPlan, rightPlan, Inner, Some(condition)))
    val joined = FramelessInternals.executePlan(dataset, join)
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

    val join = resolveSelfJoin(Join(leftPlan, rightPlan, LeftOuter, Some(condition)))
    val joined = FramelessInternals.executePlan(dataset, join)
    val leftOutput = joined.analyzed.output.take(leftPlan.output.length)
    val rightOutput = joined.analyzed.output.takeRight(rightPlan.output.length)

    val joinedPlan = Project(List(
      Alias(CreateStruct(leftOutput), "_1")(),
      Alias(CreateStruct(rightOutput), "_2")()
    ), joined.analyzed)

    val joinedDs = FramelessInternals.mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, Option[A])])

    TypedDataset.create[(T, Option[A])](joinedDs)
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
        // if column is struct, we use all it's fields
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
        val selected = dataset.toDF()
          .select(columns.toList[UntypedExpression[T]].map(c => new Column(c.expr)):_*)
          .as[Out](TypedExpressionEncoder[Out])

          TypedDataset.create[Out](selected)
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
  def drop[Out, TRep <: HList, Removed <: HList, ValuesFromRemoved <: HList, V]
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

  /**
    * Returns a new TypedDataset with the specified column updated with a new value
    *
    * @param column column given as a symbol to replace
    * @param replacement column to replace the value with
    * @param i0 Eviendence that a column with the correct type and name exists
    */
  def withColumn[A](
    column: Witness.Lt[Symbol],
    replacement: TypedColumn[T, A]
  )(implicit
    i0: TypedColumn.Exists[T, column.T, A]
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
    * @see [[frameless.TypedDataset.withColumnApply#apply]]
    */
  def withColumn[U] = new withColumnApply[U]

  class withColumnApply[U] {
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

    val reshaped = if (shouldReshape) df.toDF(targetColNames: _*) else df

    new TypedDataset[A](reshaped.as[A](TypedExpressionEncoder[A]))
  }

  /** Prefer `TypedDataset.create` over `TypedDataset.unsafeCreate` unless you
    * know what you are doing. */
  @deprecated("Prefer TypedDataset.create over TypedDataset.unsafeCreate", "0.3.0")
  def unsafeCreate[A: TypedEncoder](dataset: Dataset[A]): TypedDataset[A] = {
    new TypedDataset[A](dataset)
  }
}
