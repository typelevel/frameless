import org.apache.spark.sql.{DataFrame, Column, Row, DataFrameWriter, GroupedData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random.{nextLong => randomLong}

import shapeless._
import shapeless.nat._1
import shapeless.ops.record.{SelectAll, Values}
import shapeless.ops.hlist.{ToList, IsHCons, Tupler, Prepend}
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable.traversableOps
import shapeless.tag.@@
import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.numeric.Interval.{Closed => ClosedInterval}
import eu.timepit.refined.auto._

case class TypedFrame[Schema](df: DataFrame) {
  def as[NewSchema] = new FieldRenamer[NewSchema]
  
  class FieldRenamer[NewSchema] {
    def apply[S <: HList]()
      (implicit
        l: Generic.Aux[Schema, S],
        r: Generic.Aux[NewSchema, S]
      ): TypedFrame[NewSchema] =
       TypedFrame(df)
  }

  def toDF(): DataFrame = df
  
  def cartesianJoin[OtherSchema, L <: HList, R <: HList, P <: HList, M <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      P: Prepend.Aux[L, R, P],
      v: Values.Aux[P, V],
      t: Tupler[V]
    ): TypedFrame[t.Out] =
      TypedFrame(df.join(other.df))
  
  // def innerJoin[OtherSchema]
  // def outerJoin[OtherSchema]
  // def leftOuterJoin[OtherSchema]
  // def rightOuterJoin[OtherSchema]
  // def semiJoin[OtherSchema]
  
  // val sort = orderBy
  object orderBy extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll[G, C]
      ): TypedFrame[Schema] =
        TypedFrame(df.sort(l(columnTuple).map(c => col(c.name)): _*))
  }
  
  object select extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        t: Tupler[S]
      ): TypedFrame[t.Out] =
        TypedFrame(df.select(l(columnTuple).map(c => col(c.name)): _*))
  }
   
  def selectExpr(exprs: String*): DataFrame =
    df.selectExpr(exprs: _*)
  // def where(condition: Column) = filter(condition)
  def filter(condition: Column): DataFrame =
    df.filter(condition)
  def agg(expr: Column, exprs: Column*): DataFrame =
    df.agg(expr, exprs: _*)
  
  def limit(n: Int @@ NonNegative): TypedFrame[Schema] =
    TypedFrame(df.limit(n))
  
  def unionAll[OtherSchema, L <: HList, R <: HList, M <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      u: Union.Aux[L, R, M],
      v: Values.Aux[M, V],
      t: Tupler[V]
    ): TypedFrame[t.Out] =
      TypedFrame(df.unionAll(other.df))
  
  def intersect[OtherSchema, L <: HList, R <: HList, I <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      i: Intersection.Aux[L, R, I],
      v: Values.Aux[I, V],
      t: Tupler[V]
    ): TypedFrame[t.Out] =
      TypedFrame(df.intersect(other.df))
  
  def except[OtherSchema, L <: HList, R <: HList, D <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      d: Diff.Aux[L, R, D],
      v: Values.Aux[D, V],
      t: Tupler[V]
    ): TypedFrame[t.Out] =
      TypedFrame(df.except(other.df))
      
  def sample(
    withReplacement: Boolean,
    fraction: Double @@ ClosedInterval[_0, _1],
    seed: Long = randomLong
  ): TypedFrame[Schema] =
    TypedFrame(df.sample(withReplacement, fraction, seed))
  
  def randomSplit(
    weights: Array[Double @@ NonNegative],
    seed: Long = randomLong
  ): Array[TypedFrame[Schema]] = {
    val a: Array[Double] = weights.map(identity)
    df.randomSplit(a, seed).map(TypedFrame[Schema])
  }
  
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame =
    df.explode(input: _*)(f)
  
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B]): DataFrame = 
    df.explode(inputColumn, outputColumn)(f)
  
  object drop extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList, V <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: AllRemover.Aux[G, C, R],
        v: Values.Aux[R, V],
        t: Tupler[V]
      ): TypedFrame[t.Out] =
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
        TypedFrame(columnTuple match {
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
      ): TypedFrame[Schema] =
        TypedFrame(df.describe(l(columnTuple).map(_.name): _*))
  }
  
  def repartition(numPartitions: Int @@ Positive): TypedFrame[Schema] =
    TypedFrame(df.repartition(numPartitions))
  
  def coalesce(numPartitions: Int @@ Positive): TypedFrame[Schema] =
    TypedFrame(df.coalesce(numPartitions))
  
  def distinct(): TypedFrame[Schema] = TypedFrame(df.distinct())
  
  def persist(): TypedFrame[Schema] = TypedFrame(df.persist())
  
  def cache(): TypedFrame[Schema] = TypedFrame(df.cache())
  
  def persist(newLevel: StorageLevel): TypedFrame[Schema] = TypedFrame(df.persist(newLevel))
  
  def unpersist(blocking: Boolean): TypedFrame[Schema] = TypedFrame(df.unpersist(blocking))
  
  def unpersist(): TypedFrame[Schema] = TypedFrame(df.unpersist())
  
  def schema: StructType = df.schema
  
  def dtypes: Array[(String, String)] = df.dtypes
  
  def columns: Array[String] = df.columns
  
  def printSchema(): Unit = df.printSchema()
  
  def explain(extended: Boolean): Unit = df.explain(extended)
  
  def explain(): Unit = df.explain()
  
  def isLocal: Boolean = df.isLocal
  
  def show(numRows: Int @@ Positive = 20, truncate: Boolean = true): Unit =
    df.show(numRows, truncate)
  
  def na: TypedFrameNaFunctions[Schema] = TypedFrameNaFunctions[Schema](df.na)
  
  def stat: TypedFrameStatFunctions[Schema] = TypedFrameStatFunctions[Schema](df.stat)
  
  object groupBy extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): GroupedData = // Tricky
        df.groupBy(l(columnTuple).map(c => col(c.name)): _*)
  }
  
  object rollup extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): GroupedData = // Tricky
        df.rollup(l(columnTuple).map(c => col(c.name)): _*)
  }
  
  object cube extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): GroupedData = // Tricky
        df.cube(l(columnTuple).map(c => col(c.name)): _*)
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

  def map[R: ClassTag](f: Row => R): RDD[R] =
    df.map(f)
  def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] =
    df.flatMap(f)
  def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] =
    df.mapPartitions(f)
  def foreach(f: Row => Unit): Unit =
    df.foreach(f)
  def foreachPartition(f: Iterator[Row] => Unit): Unit =
    df.foreachPartition(f)
  
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
