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
import shapeless.ops.hlist.{ToList, ToTraversable, IsHCons, Prepend, RemoveAll, Length}
import shapeless.tag.@@

import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.numeric.Interval.{Closed => ClosedInterval}
import eu.timepit.refined.auto._

final class TypedFrame[Schema <: Product] private[typedframe]
  (_df: DataFrame)
  (implicit val fields: Fields[Schema])
    extends Serializable {
  
  val df = _df.toDF(fields(): _*)
  
  def as[NewSchema <: Product] = new FieldRenamer[NewSchema]
  
  class FieldRenamer[NewSchema <: Product] {
    def apply[S <: HList]()
      (implicit
        l: Generic.Aux[Schema, S],
        r: Generic.Aux[NewSchema, S],
        g: Fields[NewSchema]
      ): TypedFrame[NewSchema] =
       new TypedFrame(df)
  }
  
  def rdd(implicit t: TypeableRow[Schema], l: ClassTag[Schema]): RDD[Schema] = df.map(t.apply)
  
  def cartesianJoin[OtherSchema <: Product, Out <: Product, L <: HList, R <: HList, P <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      P: Prepend.Aux[L, R, P],
      v: Values.Aux[P, V],
      t: XLTupler.Aux[V, Out],
      g: Fields[Out]
    ): TypedFrame[Out] =
      new TypedFrame(df.join(other.df))
  
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
    def usingProduct[C <: HList, Out <: Product, L <: HList, R <: HList, S <: HList, A <: HList, V <: HList, D <: HList, P <: HList]
      (columns: C)
      (implicit
        hc: IsHCons[C],
        fl: FieldsExtractor.Aux[Schema, C, L, S],
        fr: FieldsExtractor.Aux[OtherSchema, C, R, S],
        rc: AllRemover.Aux[R, C, A],
        vc: Values.Aux[L, V],
        vd: Values.Aux[A, D],
        p: Prepend.Aux[V, D, P],
        t: XLTupler.Aux[P, Out],
        g: Fields[Out]
      ): TypedFrame[Out] = {
        val joinColumns: Seq[String] = fl(columns)
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
        
        new TypedFrame(prefixed.select(slefColumns ++ otherColumns: _*))
      }
    
    def onProduct[C <: HList](columns: C): JoinOnPartial[C, OtherSchema] =
      new JoinOnPartial[C, OtherSchema](columns: C, other: TypedFrame[OtherSchema], joinType: String)
  }
  
  class JoinOnPartial[C <: HList, OtherSchema <: Product](columns: C, other: TypedFrame[OtherSchema], joinType: String)
      extends SingletonProductArgs {
    def andProduct[D <: HList, Out <: Product, L <: HList, R <: HList, S <: HList, V <: HList, W <: HList, P <: HList]
      (otherColumnTuple: D)
      (implicit
        hc: IsHCons[C],
        fl: FieldsExtractor.Aux[Schema, C, L, S],
        fr: FieldsExtractor.Aux[Schema, D, R, S],
        vc: Values.Aux[L, V],
        vd: Values.Aux[R, W],
        p: Prepend.Aux[V, W, P],
        t: XLTupler.Aux[P, Out],
        g: Fields[Out]
      ): TypedFrame[Out] = {
        val expr = fl(columns).map(df(_))
          .zip(fr(otherColumnTuple).map(other.df(_)))
          .map(z => z._1 === z._2)
          .reduce(_ && _)
        new TypedFrame(df.join(other.df, expr, joinType))
      }
  }
  
  object sort extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor[Schema, C]
      ): TypedFrame[Schema] =
        new TypedFrame(df.sort(f(columns).map(col): _*))
  }
  
  object sortDesc extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor[Schema, C]
      ): TypedFrame[Schema] =
        new TypedFrame(df.sort(f(columns).map(c => col(c).desc): _*))
  }
  
  object select extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, S <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor.Aux[Schema, C, G, S],
        t: XLTupler.Aux[S, Out],
        b: Fields[Out]
      ): TypedFrame[Out] =
        new TypedFrame(df.select(f(columns).map(col): _*))
  }
  
  def selectExpr(exprs: String*): DataFrame =
    df.selectExpr(exprs: _*)
  
  def filter
    (f: Schema => Boolean)
    (implicit
      s: SQLContext,
      t: TypeableRow[Schema]
    ): TypedFrame[Schema] =
      new TypedFrame(s.createDataFrame(df.rdd.filter(r => f(t(r))), df.schema))
  
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
  
  def explode[NewSchema <: Product, Out <: Product, N <: HList, G <: HList, P <: HList]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      a: TypeTag[NewSchema],
      n: Generic.Aux[NewSchema, N],
      t: TypeableRow.Aux[Schema, G],
      p: Prepend.Aux[G, N, P],
      m: XLTupler.Aux[P, Out],
      g: Fields[Out]
    ): TypedFrame[Out] =
      new TypedFrame(df.explode(df.columns.map(col): _*)(r => f(t(r))))
      
  object drop extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, R <: HList, V <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor.Aux[Schema, C, G, _],
        r: AllRemover.Aux[G, C, R],
        v: Values.Aux[R, V],
        t: XLTupler.Aux[V, Out],
        b: Fields[Out]
      ): TypedFrame[Out] =
        new TypedFrame(f(columns).foldLeft(df)(_ drop _))
  }
  
  object dropDuplicates extends SingletonProductArgs {
    def applyProduct[C <: HList](columns: C)(implicit f: FieldsExtractor[Schema, C]): TypedFrame[Schema] =
      new TypedFrame(columns match {
        case HNil => df.dropDuplicates()
        case _ => df.dropDuplicates(f(columns))
      })
  }
  
  object describe extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor[Schema, C]
      ): DataFrame =
        df.describe(f(columns): _*)
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
    def applyProduct[C <: HList](columns: C)
      (implicit f: FieldsExtractor[Schema, C]): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.groupBy(f(columns).map(col): _*))
  }
  
  object rollup extends SingletonProductArgs {
    def applyProduct[C <: HList](columns: C)
      (implicit f: FieldsExtractor[Schema, C]): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.rollup(f(columns).map(col): _*))
  }
  
  object cube extends SingletonProductArgs {
    def applyProduct[C <: HList](columns: C)
      (implicit f: FieldsExtractor[Schema, C]): GroupedTypedFrame[Schema, C] =
        new GroupedTypedFrame(df.cube(f(columns).map(col): _*))
  }
  
  def head()(implicit t: TypeableRow[Schema]): Schema = t(df.head())
  
  def take(n: Int @@ NonNegative)(implicit t: TypeableRow[Schema]): Seq[Schema] =
    df.head(n).map(t.apply)
  
  def reduce(f: (Schema, Schema) => Schema)(implicit t: TypeableRow[Schema], l: ClassTag[Schema]): Schema =
    rdd.reduce(f)
  
  def map[NewSchema <: Product]
    (f: Schema => NewSchema)
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: Fields[NewSchema]
    ): TypedFrame[NewSchema] =
      new TypedFrame(s.createDataFrame(df.map(r => f(w(r)))))
  
  def flatMap[NewSchema <: Product]
    (f: Schema => TraversableOnce[NewSchema])
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: Fields[NewSchema]
    ): TypedFrame[NewSchema] =
      new TypedFrame(s.createDataFrame(df.flatMap(r => f(w(r)))))
  
  def mapPartitions[NewSchema <: Product]
    (f: Iterator[Schema] => Iterator[NewSchema])
    (implicit
      s: SQLContext,
      w: TypeableRow[Schema],
      c: ClassTag[NewSchema],
      t: TypeTag[NewSchema],
      b: Fields[NewSchema]
    ): TypedFrame[NewSchema] =
      new TypedFrame(s.createDataFrame(df.mapPartitions(i => f(i.map(w.apply)))))
  
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
