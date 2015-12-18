package typedframe

import org.apache.spark.sql.{DataFrame, Column, Row, DataFrameWriter, GroupedData, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random.{nextLong => randomLong}
import scala.spores._

import shapeless._
import shapeless.nat._1
import shapeless.ops.record.{SelectAll, Values, Keys}
import shapeless.ops.hlist.{ToList, ToTraversable, IsHCons, Prepend, RemoveAll}
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable.traversableOps
import shapeless.tag.@@
import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.numeric.Interval.{Closed => ClosedInterval}
import eu.timepit.refined.auto._

object TypedFrame {
  def apply[S <: Product, B <: HList, Y <: HList]
    (dataFrame: DataFrame)
    (implicit
      b: LabelledGeneric.Aux[S, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[S] =
      new TypedFrame[S](dataFrame.toDF(y().toList.map(_.name): _*))
}

final class TypedFrame[Schema <: Product] private[typedframe] (val df: DataFrame) extends Serializable {
  
  def as[NewSchema <: Product] = new FieldRenamer[NewSchema]
  
  class FieldRenamer[NewSchema <: Product] {
    def apply[S <: HList, L <: HList, K <: HList, B <: HList, Y <: HList]()
      (implicit
        l: Generic.Aux[Schema, S],
        r: Generic.Aux[NewSchema, S],
        b: LabelledGeneric.Aux[NewSchema, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[NewSchema] =
       TypedFrame(df)
  }

  def rdd[L <: HList]
    (implicit
      c: ClassTag[Schema],
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L]
    ): RDD[Schema] =
      df.map(rowToSchema[L])
  
  def cartesianJoin[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, P <: HList, V <: HList, B <: HList, Y <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      P: Prepend.Aux[L, R, P],
      v: Values.Aux[P, V],
      t: ManyTupler.Aux[V, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.join(other.df))
  
  def innerJoin[OtherSchema <: Product](other: TypedFrame[OtherSchema]) =
    new JoinPartial(other, "inner")
  
  def outerJoin[OtherSchema <: Product](other: TypedFrame[OtherSchema]) =
    new JoinPartial(other, "outer")
  
  def leftOuterJoin[OtherSchema <: Product](other: TypedFrame[OtherSchema]) =
    new JoinPartial(other, "left_outer")
  
  def rightOuterJoin[OtherSchema <: Product](other: TypedFrame[OtherSchema]) =
    new JoinPartial(other, "right_outer")
  
  def leftsemiJoin[OtherSchema <: Product](other: TypedFrame[OtherSchema]) =
    new JoinPartial(other, "leftsemi")
  
  class JoinPartial[OtherSchema <: Product](other: TypedFrame[OtherSchema], joinType: String) extends SingletonProductArgs {
    def usingProduct[C <: HList, Out <: Product, L <: HList, R <: HList, S <: HList, T <: HList, A <: HList, V <: HList, D <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        hc: IsHCons[C],
        tc: ToList[C, Symbol],
        gc: LabelledGeneric.Aux[Schema, L],
        gd: LabelledGeneric.Aux[OtherSchema, R],
        sc: SelectAll.Aux[L, C, S],
        sd: SelectAll.Aux[R, C, T],
        ev: S =:= T,
        rc: AllRemover.Aux[R, C, A],
        vc: Values.Aux[L, V],
        vd: Values.Aux[A, D],
        p: Prepend.Aux[V, D, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] = {
        val columns = tc(columnTuple).map(_.name)
        val expr = columns.map(n => df(n) === other.df(n)).reduce(_ && _)
        val joined = df.join(other.df, expr, joinType)
        TypedFrame(columns.map(other.df(_)).foldLeft(joined)(_ drop _))
      }
    
    def onProduct[C <: HList](columnTuple: C): JoinOnPartial[C, OtherSchema] =
      new JoinOnPartial[C, OtherSchema](columnTuple: C, other: TypedFrame[OtherSchema], joinType: String)
  }
  
  class JoinOnPartial[C <: HList, OtherSchema <: Product](columnTuple: C, other: TypedFrame[OtherSchema], joinType: String)
      extends SingletonProductArgs {
    def andProduct[D <: HList, L <: HList, R <: HList, S <: HList, T <: HList, V <: HList, W <: HList, P <: HList, Out <: Product, B <: HList, Y <: HList]
      (otherColumnTuple: D)
      (implicit
        hc: IsHCons[C],
        tc: ToList[C, Symbol],
        td: ToList[D, Symbol],
        gc: LabelledGeneric.Aux[Schema, L],
        gd: LabelledGeneric.Aux[OtherSchema, R],
        sc: SelectAll.Aux[L, C, S],
        sd: SelectAll.Aux[R, D, T],
        ev: S =:= T,
        vc: Values.Aux[L, V],
        vd: Values.Aux[R, W],
        p: Prepend.Aux[V, W, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] = {
        val expr = tc(columnTuple).map(_.name).map(df(_))
          .zip(td(otherColumnTuple).map(_.name).map(other.df(_)))
          .map(z => z._1 === z._2)
          .reduce(_ && _)
        TypedFrame(df.join(other.df, expr, joinType))
      }
  }
  
  // val sort = orderBy
  object orderBy extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll[G, C],
        y: Keys.Aux[G, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Schema] =
        TypedFrame(df.sort(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  object select extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, S <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        t: ManyTupler.Aux[S, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(df.select(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  def selectExpr(exprs: String*): DataFrame =
    df.selectExpr(exprs: _*)
  
  // def where(condition: Column) = filter(condition)
  def filter[L <: HList, B <: HList, Y <: HList]
    (f: Schema => Boolean)
    (implicit
      s: SQLContext,
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L],
      b: LabelledGeneric.Aux[Schema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Schema] =
      TypedFrame(s.createDataFrame(df.rdd.filter(r => f(rowToSchema[L](r))), df.schema))
  
  def limit(n: Int @@ NonNegative): TypedFrame[Schema] =
    new TypedFrame(df.limit(n))
  
  def unionAll[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, M <: HList, V <: HList, B <: HList, Y <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      u: Union.Aux[L, R, M],
      v: Values.Aux[M, V],
      t: ManyTupler.Aux[V, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.unionAll(other.df))
  
  def intersect[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, I <: HList, V <: HList, B <: HList, Y <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      i: Intersection.Aux[L, R, I],
      v: Values.Aux[I, V],
      t: ManyTupler.Aux[V, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.intersect(other.df))
  
  def except[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, D <: HList, V <: HList, B <: HList, Y <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      d: Diff.Aux[L, R, D],
      v: Values.Aux[D, V],
      t: ManyTupler.Aux[V, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.except(other.df))
      
  def sample(
    withReplacement: Boolean,
    fraction: Double @@ ClosedInterval[_0, _1],
    seed: Long = randomLong
  ): TypedFrame[Schema] =
    new TypedFrame(df.sample(withReplacement, fraction, seed))
  
  def randomSplit(
    weights: Array[Double @@ NonNegative],
    seed: Long = randomLong
  ): Array[TypedFrame[Schema]] = {
    val a: Array[Double] = weights.map(identity)
    df.randomSplit(a, seed).map(d => new TypedFrame[Schema](d))
  }
  
  def explode[NewSchema <: Product, Out <: Product, N <: HList, L <: HList, P <: HList, B <: HList, Y <: HList]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      a: TypeTag[NewSchema],
      n: Generic.Aux[NewSchema, N],
      g: Generic.Aux[Schema, L],
      v: FromTraversable[L],
      p: Prepend.Aux[L, N, P],
      t: ManyTupler.Aux[P, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.explode(df.columns.map(col): _*)(r => f(rowToSchema[L](r))))
      
  object drop extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, R <: HList, V <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: AllRemover.Aux[G, C, R],
        v: Values.Aux[R, V],
        t: ManyTupler.Aux[V, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(l(columnTuple).map(_.name).foldLeft(df)(_ drop _))
  }
  
  object dropDuplicates extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): TypedFrame[Schema] =
        new TypedFrame(columnTuple match {
          case HNil => df.dropDuplicates()
          case _ => df.dropDuplicates(l(columnTuple).map(_.name))
        })
  }
  
  object describe extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): DataFrame =
        df.describe(l(columnTuple).map(_.name): _*)
  }
  
  def repartition(numPartitions: Int @@ Positive): TypedFrame[Schema] =
    new TypedFrame(df.repartition(numPartitions))
  
  def coalesce(numPartitions: Int @@ Positive): TypedFrame[Schema] =
    new TypedFrame(df.coalesce(numPartitions))
  
  def distinct(): TypedFrame[Schema] = new TypedFrame(df.distinct())
  
  def persist(): TypedFrame[Schema] = new TypedFrame(df.persist())
  
  def cache(): TypedFrame[Schema] = new TypedFrame(df.cache())
  
  def persist(newLevel: StorageLevel): TypedFrame[Schema] = new TypedFrame(df.persist(newLevel))
  
  def unpersist(blocking: Boolean): TypedFrame[Schema] = new TypedFrame(df.unpersist(blocking))
  
  def unpersist(): TypedFrame[Schema] = new TypedFrame(df.unpersist())
  
  def schema: StructType = df.schema
  
  def dtypes: Array[(String, String)] = df.dtypes
  
  def columns: Array[String] = df.columns
  
  def printSchema(): Unit = df.printSchema()
  
  def explain(extended: Boolean): Unit = df.explain(extended)
  
  def explain(): Unit = df.explain()
  
  def isLocal: Boolean = df.isLocal
  
  def show(numRows: Int @@ Positive = 20, truncate: Boolean = true): Unit =
    df.show(numRows, truncate)
  
  def na: TypedFrameNaFunctions[Schema] = new TypedFrameNaFunctions(df.na)
  
  def stat: TypedFrameStatFunctions[Schema] = new TypedFrameStatFunctions(df.stat)
  
  object groupBy extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, V <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        v: Values.Aux[G, V],
        r: AllRemover.Aux[G, C, R]
      ): GroupedTypedFrame[V, R] =
        new GroupedTypedFrame(df.groupBy(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  object rollup extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, V <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        v: Values.Aux[G, V],
        r: AllRemover.Aux[G, C, R]
      ): GroupedTypedFrame[V, R] =
        new GroupedTypedFrame(df.rollup(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  object cube extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, V <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        v: Values.Aux[G, V],
        r: AllRemover.Aux[G, C, R]
      ): GroupedTypedFrame[V, R] =
        new GroupedTypedFrame(df.cube(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  // def first = head
  def head[G <: HList]()
    (implicit
      g: Generic.Aux[Schema, G],
      f: FromTraversable[G]
    ): Schema =
      g.from(df.head().toSeq.toHList[G].get)
  
  // def head(n: Int) = take(n)
  def take[G <: HList]
    (n: Int @@ NonNegative)
    (implicit
      g: Generic.Aux[Schema, G],
      f: FromTraversable[G]
    ): Seq[Schema] =
      df.head(n).map(r => g.from(r.toSeq.toHList[G].get))
  
  def reduce[L <: HList]
    (f: (Schema, Schema) => Schema)
    (implicit
      c: ClassTag[Schema],
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L]
    ): Schema =
      rdd.reduce(f)

  def map[NewSchema <: Product, L <: HList, B <: HList, Y <: HList]
    (f: Schema => NewSchema)
    (implicit
      s: SQLContext,
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.map(r => f(rowToSchema[L](r)))))
  
  def flatMap[NewSchema <: Product, L <: HList, B <: HList, Y <: HList]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      s: SQLContext,
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.flatMap(r => f(rowToSchema[L](r)))))
  
  def mapPartitions[NewSchema <: Product, L <: HList, B <: HList, Y <: HList]
    (f: Iterator[Schema] => Iterator[NewSchema])
    (implicit
      s: SQLContext,
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.mapPartitions(i => f(i.map(rowToSchema[L])))))
  
  def foreach[L <: HList]
    (f: Schema => Unit)
    (implicit
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L]
    ): Unit =
      df.foreach(r => f(rowToSchema[L](r)))
  
  def foreachPartition[L <: HList]
    (f: Iterator[Schema] => Unit)
    (implicit
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L]
    ): Unit =
      df.foreachPartition(i => f(i.map(rowToSchema[L])))
  
  private def rowToSchema[L <: HList]
    (row: Row)
    (implicit
      g: Generic.Aux[Schema, L],
      V: FromTraversable[L]
    ): Schema =
      g.from(row.toSeq.toHList[L].get)

  def collect[G <: HList]()
    (implicit
      g: Generic.Aux[Schema, G],
      f: FromTraversable[G]
    ): Seq[Schema] =
      df.collect().map(r => g.from(r.toSeq.toHList[G].get))
  
  def count(): Long = df.count()
  
  def registerTempTable(tableName: String): Unit = df.registerTempTable(tableName)
  
  def write: DataFrameWriter = df.write
  
  def toJSON: RDD[String] = df.toJSON
  
  def inputFiles: Array[String] = df.inputFiles
}
