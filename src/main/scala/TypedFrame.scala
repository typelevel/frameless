package typedframe

import org.apache.spark.sql.{DataFrame, Column, Row, DataFrameWriter, GroupedData, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random.{nextLong => randomLong}

import shapeless._
import shapeless.nat._1
import shapeless.ops.traversable.FromTraversable
import shapeless.ops.record.{SelectAll, Values, Keys}
import shapeless.ops.hlist.{ToList, ToTraversable, IsHCons, Prepend, RemoveAll, Length, Fill}
import shapeless.tag.@@

import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.numeric.Interval.{Closed => ClosedInterval}
import eu.timepit.refined.auto._

// TODO: Factor out {LabelledGeneric, Keys, ToList} as a TC
object TypedFrame {
  def apply[S <: Product, B <: HList, Y <: HList]
    (dataFrame: DataFrame)
    (implicit
      b: LabelledGeneric.Aux[S, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[S] =
      new TypedFrame[S](dataFrame.toDF(y().toList.map(_.name): _*))

  def mk[S <: Product](dataFrame: DataFrame)(implicit l: LabelledGenericListable[S]): TypedFrame[S] =
    new TypedFrame[S](dataFrame.toDF(l.list: _*))
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

  def rdd(implicit t: TypeableRow[Schema], l: ClassTag[Schema]): RDD[Schema] = df.map(t.apply)
  
  def cartesianJoin[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, P <: HList, V <: HList, B <: HList, Y <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      P: Prepend.Aux[L, R, P],
      v: Values.Aux[P, V],
      t: XLTupler.Aux[V, Out],
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
        t: XLTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] = {
        val joinColumns: Seq[String] = tc(columnTuple).map(_.name)
        val joinExpr: Column = joinColumns.map(n => df(n) === other.df(n)).reduce(_ && _)
        val joined: DataFrame = df.join(other.df, joinExpr, joinType)
        
        val slefCol: String => String = "s".+
        val othrCol: String => String = "o".+
        val prefixed: DataFrame = joined.toDF(df.columns.map(slefCol) ++ other.df.columns.map(othrCol): _*)
        
        val slefColumns: Seq[Column] = df.columns.map { c =>
          if (joinColumns.contains(c))
            when(
              prefixed(slefCol(c)).isNotNull,
              prefixed(slefCol(c))
            ) otherwise
              prefixed(othrCol(c))
          else prefixed(slefCol(c))
        }
        
        val otherColumns: Seq[Column] =
          other.df.columns.filterNot(joinColumns.contains).map(c => prefixed(othrCol(c)))
        
        TypedFrame(prefixed.select(slefColumns ++ otherColumns: _*))
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
        t: XLTupler.Aux[P, Out],
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
        TypedFrame(df.sort(columnTuple.toList.map(c => col(c.name)): _*))
  }
  
  object select extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, S <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        t: XLTupler.Aux[S, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(df.select(columnTuple.toList.map(c => col(c.name)): _*))
  }
  
  def selectExpr(exprs: String*): DataFrame =
    df.selectExpr(exprs: _*)
  
  // def where(condition: Column) = filter(condition)
  def filter[B <: HList, Y <: HList]
    (f: Schema => Boolean)
    (implicit
      s: SQLContext,
      t: TypeableRow[Schema],
      b: LabelledGeneric.Aux[Schema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Schema] =
      TypedFrame(s.createDataFrame(df.rdd.filter(r => f(t(r))), df.schema))
  
  def limit(n: Int @@ NonNegative): TypedFrame[Schema] =
    new TypedFrame(df.limit(n))
  
  def unionAll[OtherSchema <: Product, S <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: Generic.Aux[Schema, S],
      r: Generic.Aux[OtherSchema, S]
    ): TypedFrame[Schema] =
      new TypedFrame(df.unionAll(other.df))
  
  def intersect[OtherSchema <: Product, S <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: Generic.Aux[Schema, S],
      r: Generic.Aux[OtherSchema, S]
    ): TypedFrame[Schema] =
      new TypedFrame(df.intersect(other.df))
  
  def except[OtherSchema <: Product, S <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: Generic.Aux[Schema, S],
      r: Generic.Aux[OtherSchema, S]
    ): TypedFrame[Schema] =
      new TypedFrame(df.except(other.df))
      
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
  
  def explode[NewSchema <: Product, Out <: Product, N <: HList, G <: HList, P <: HList, B <: HList, Y <: HList]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      a: TypeTag[NewSchema],
      n: Generic.Aux[NewSchema, N],
      t: TypeableRow[Schema],
      p: Prepend.Aux[G, N, P],
      m: XLTupler.Aux[P, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(df.explode(df.columns.map(col): _*)(r => f(t(r))))
      
  object drop extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, R <: HList, V <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: AllRemover.Aux[G, C, R],
        v: Values.Aux[R, V],
        t: XLTupler.Aux[V, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(columnTuple.toList.map(_.name).foldLeft(df)(_ drop _))
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
          case _ => df.dropDuplicates(columnTuple.toList.map(_.name))
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
        df.describe(columnTuple.toList.map(_.name): _*)
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
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll[G, C]
      ): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.groupBy(columnTuple.toList.map(c => col(c.name)): _*))
  }
  
  object rollup extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll[G, C]
      ): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.rollup(columnTuple.toList.map(c => col(c.name)): _*))
  }
  
  object cube extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll[G, C]
      ): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.cube(columnTuple.toList.map(c => col(c.name)): _*))
  }
  
  // def first = head
  def head()(implicit t: TypeableRow[Schema]): Schema = t(df.head())
  
  // def head(n: Int) = take(n)
  def take(n: Int @@ NonNegative)(implicit t: TypeableRow[Schema]): Seq[Schema] =
    df.head(n).map(t.apply)
  
  def reduce(f: (Schema, Schema) => Schema)(implicit t: TypeableRow[Schema], l: ClassTag[Schema]): Schema =
    rdd.reduce(f)
  
  def map[NewSchema <: Product, B <: HList, Y <: HList]
    (f: Schema => NewSchema)
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.map(r => f(w(r)))))
  
  def flatMap[NewSchema <: Product, B <: HList, Y <: HList]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.flatMap(r => f(w(r)))))
  
  def mapPartitions[NewSchema <: Product, B <: HList, Y <: HList]
    (f: Iterator[Schema] => Iterator[NewSchema])
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: LabelledGeneric.Aux[NewSchema, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[NewSchema] =
      TypedFrame(s.createDataFrame(df.mapPartitions(i => f(i.map(w.apply)))))
  
  def foreach(f: Schema => Unit)(implicit t: TypeableRow[Schema]): Unit =
    df.foreach(r => f(t(r)))
  
  def foreachPartition(f: Iterator[Schema] => Unit)(implicit t: TypeableRow[Schema]): Unit =
    df.foreachPartition(i => f(i.map(t.apply)))
  
  def collect()(implicit t: TypeableRow[Schema]): Seq[Schema] = df.collect().map(t.apply)
  
  def count(): Long = df.count()
  
  def registerTempTable(tableName: String): Unit = df.registerTempTable(tableName)
  
  def write: DataFrameWriter = df.write
  
  def toJSON: RDD[String] = df.toJSON
  
  def inputFiles: Array[String] = df.inputFiles
  
}
