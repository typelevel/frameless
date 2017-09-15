package frameless

import frameless.ops._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter, FullOuter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import shapeless._
import shapeless.ops.hlist.{Prepend, ToTraversable, Tupler}
import CanAccess.localCanAccessInstance

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
    TypedEncoder[A].targetDataType match {
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
    def applyProduct[U <: HList, Out0 <: HList, Out](columns: U)(
      implicit
      tc: AggregateTypes.Aux[T, U, Out0],
      toTraversable: ToTraversable.Aux[U, List, UntypedExpression[T]],
      tupler: Tupler.Aux[Out0, Out],
      encoder: TypedEncoder[Out]
    ): TypedDataset[Out] = {

      val cols = toTraversable(columns).map(c => new Column(c.expr))

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
    * Differs from `Dataset#count` by wrapping it's result into a [[Job]].
    */
  def count(): Job[Long] =
    Job(dataset.count)

  /** Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    * tf('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def apply[A](column: Witness.Lt[Symbol])(
    implicit
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]
  ): TypedColumn[T, A] = col(column)

  /** Returns `TypedColumn` of type `A` given it's name.
    *
    * {{{
    * tf.col('id)
    * }}}
    *
    * It is statically checked that column with such name exists and has type `A`.
    */
  def col[A, X](column: Witness.Lt[Symbol])(
    implicit
    ca: CanAccess[T, X],
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]
  ): TypedColumn[X, A] = {
    val colExpr = dataset.col(column.value.name).as[A](TypedExpressionEncoder[A])
    new TypedColumn[X, A](colExpr)
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

  /** Computes the inner join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinInner[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] = {
      import FramelessInternals._
      val leftPlan = logicalPlan(dataset)
      val rightPlan = logicalPlan(other.dataset)
      val join = resolveSelfJoin(Join(leftPlan, rightPlan, Inner, Some(condition(localCanAccessInstance).expr)))
      val joinedPlan = joinPlan(dataset, join, leftPlan, rightPlan)
      val joinedDs = mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, U)])
      TypedDataset.create[(T, U)](joinedDs)
    }

  /** Computes the cartesian project of `this` `Dataset` with the `other` `Dataset` */
  def joinCross[U](other: TypedDataset[U])
    (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
      new TypedDataset(self.dataset.joinWith(other.dataset, new Column(Literal(true)), "cross"))

  /** Computes the full outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinFull[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(Option[T], Option[U])]): TypedDataset[(Option[T], Option[U])] = {
      import FramelessInternals._
      val leftPlan = logicalPlan(dataset)
      val rightPlan = logicalPlan(other.dataset)
      val join = resolveSelfJoin(Join(leftPlan, rightPlan, FullOuter, Some(condition(localCanAccessInstance).expr)))
      val joinedPlan = joinPlan(dataset, join, leftPlan, rightPlan)
      val joinedDs = mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(Option[T], Option[U])])
      TypedDataset.create[(Option[T], Option[U])](joinedDs)
    }

  /** Computes the right outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinRight[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(Option[T], U)]): TypedDataset[(Option[T], U)] = {
      import FramelessInternals._
      val leftPlan = logicalPlan(dataset)
      val rightPlan = logicalPlan(other.dataset)
      val join = resolveSelfJoin(Join(leftPlan, rightPlan, RightOuter, Some(condition(localCanAccessInstance).expr)))
      val joinedPlan = joinPlan(dataset, join, leftPlan, rightPlan)
      val joinedDs = mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(Option[T], U)])
      TypedDataset.create[(Option[T], U)](joinedDs)
    }

  /** Computes the left outer join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeft[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean])
    (implicit e: TypedEncoder[(T, Option[U])]): TypedDataset[(T, Option[U])] = {
      import FramelessInternals._
      val leftPlan = logicalPlan(dataset)
      val rightPlan = logicalPlan(other.dataset)
      val join = resolveSelfJoin(Join(leftPlan, rightPlan, LeftOuter, Some(condition(localCanAccessInstance).expr)))
      val joinedPlan = joinPlan(dataset, join, leftPlan, rightPlan)
      val joinedDs = mkDataset(dataset.sqlContext, joinedPlan, TypedExpressionEncoder[(T, Option[U])])

      TypedDataset.create[(T, Option[U])](joinedDs)
    }

  /** Computes the left semi join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeftSemi[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    new TypedDataset(self.dataset.join(other.dataset, condition(localCanAccessInstance).untyped, "leftsemi")
      .as[T](TypedExpressionEncoder(encoder)))

  /** Computes the left anti join of `this` `Dataset` with the `other` `Dataset`,
    * returning a `Tuple2` for each pair where condition evaluates to true.
    */
  def joinLeftAnti[U](other: TypedDataset[U])(condition: CanAccess[Any, T with U] => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    new TypedDataset(self.dataset.join(other.dataset, condition(localCanAccessInstance).untyped, "leftanti")
      .as[T](TypedExpressionEncoder(encoder)))

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

  /** Takes a function from A => R and converts it to a UDF for TypedColumn[A] => TypedColumn[R].
    */
  def makeUDF[A: TypedEncoder, R: TypedEncoder](f: A => R):
    TypedColumn[T, A] => TypedColumn[T, R] =
      functions.udf(f)

  /** Takes a function from (A1, A2) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, R: TypedEncoder](f: (A1, A2) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2]) => TypedColumn[T, R] =
      functions.udf(f)

  /** Takes a function from (A1, A2, A3) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3]) => TypedColumn[T, R] =
      functions.udf(f)

  /** Takes a function from (A1, A2, A3, A4) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, A4: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3, A4) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4]) => TypedColumn[T, R] =
      functions.udf(f)

  /** Takes a function from (A1, A2, A3, A4, A5) => R and converts it to a UDF for
    * (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R].
    */
  def makeUDF[A1: TypedEncoder, A2: TypedEncoder, A3: TypedEncoder, A4: TypedEncoder, A5: TypedEncoder, R: TypedEncoder](f: (A1, A2, A3, A4, A5) => R):
    (TypedColumn[T, A1], TypedColumn[T, A2], TypedColumn[T, A3], TypedColumn[T, A4], TypedColumn[T, A5]) => TypedColumn[T, R] =
      functions.udf(f)

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

    TypedEncoder[A].targetDataType match {
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
    implicit val (ea,eb) = (ca.uencoder, cb.uencoder)

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
    def applyProduct[U <: HList, Out0 <: HList, Out](columns: U)(
      implicit
      ct: ColumnTypes.Aux[T, U, Out0],
      toTraversable: ToTraversable.Aux[U, List, UntypedExpression[T]],
      tupler: Tupler.Aux[Out0, Out],
      encoder: TypedEncoder[Out]
    ): TypedDataset[Out] = {
      val selected = dataset.toDF()
        .select(toTraversable(columns).map(c => new Column(c.expr)):_*)
        .as[Out](TypedExpressionEncoder[Out])

      TypedDataset.create[Out](selected)
    }
  }

  /** Prepends a new column to the Dataset.
    *
    * {{{
    *   case class X(i: Int, j: Int)
    *   val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    *   val fNew: TypedDataset[(Int,Int,Boolean)] = f.withColumn(f('j) === 10)
    * }}}
    */
  def withColumn[A: TypedEncoder, H <: HList, FH <: HList, Out](ca: TypedColumn[T, A])(
    implicit
    genOfA: Generic.Aux[T, H],
    init: Prepend.Aux[H, A :: HNil, FH],
    tupularFormForFH: Tupler.Aux[FH, Out],
    encoder: TypedEncoder[Out]
  ): TypedDataset[Out] = {
    // Giving a random name to the new column (the proper name will be given by the Tuple-based encoder)
    val selected = dataset.toDF().withColumn("I1X3T9CU1OP0128JYIO76TYZZA3AXHQ18RMI", ca.untyped)
      .as[Out](TypedExpressionEncoder[Out])

    TypedDataset.create[Out](selected)
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
