import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random.{nextLong => randomLong}

import shapeless._
import shapeless.ops.record._
import shapeless.ops.hlist.{Selector => _, _}

case class TypedFrame[Schema](df: DataFrame) {
  def as[NewSchema] = new FieldRenamer[Schema, NewSchema](df)
  
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
  
  // def sort = orderBy
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
    
  // def selectExpr(exprs: String*): DataFrame =
  //   df.selectExpr(exprs: _*)
  // def filter(condition: Column): DataFrame =
  //   df.filter(condition)
  // def where(condition: Column): DataFrame =
  //   df.where(condition)
  // def agg(expr: Column, exprs: Column*): DataFrame =
  //   df.agg(expr, exprs: _*)
  
  def limit(n: Int): TypedFrame[Schema] =
    TypedFrame(df.limit(n))
  
  def unionAll[OtherSchema, L <: HList, R <: HList, M <: HList, V <: HList]
    (other: TypedFrame[OtherSchema])
    (implicit
      l: LabelledGeneric.Aux[Schema, L],
      r: LabelledGeneric.Aux[OtherSchema, R],
      u: Merger.Aux[L, R, M],
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
      
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = randomLong): TypedFrame[Schema] =
    TypedFrame(df.sample(withReplacement, fraction, seed))
  
  def randomSplit(weights: Array[Double], seed: Long = randomLong): Array[TypedFrame[Schema]] =
    df.randomSplit(weights, seed).map(TypedFrame[Schema])
  
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame =
    df.explode(input: _*)(f)
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B]): DataFrame = 
    df.explode(inputColumn, outputColumn)(f)
  
  object drop extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, R <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: RemoveAll.Aux[G, C, R],
        t: Tupler[R]
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
  
  def repartition(numPartitions: Int): TypedFrame[Schema] = TypedFrame(df.repartition(numPartitions))
  def coalesce(numPartitions: Int): TypedFrame[Schema] = TypedFrame(df.coalesce(numPartitions))
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
  def explain(extended: Boolean): Unit = df.explain(extended: Boolean)
  def explain(): Unit = df.explain()
  def isLocal: Boolean = df.isLocal
  def show(numRows: Int): Unit = df.show(numRows: Int)
  def show(): Unit = df.show()
  def show(truncate: Boolean): Unit = df.show(truncate: Boolean)
  def show(numRows: Int, truncate: Boolean): Unit = df.show(numRows: Int, truncate: Boolean)
  
  def na: DataFrameNaFunctions = df.na
  def stat: DataFrameStatFunctions = df.stat
  
  def groupBy(cols: Column*): GroupedData =
    df.groupBy(cols: _*)
  def rollup(cols: Column*): GroupedData =
    df.rollup(cols: _*)
  def cube(cols: Column*): GroupedData =
    df.cube(cols: _*)
  def groupBy(col1: String, cols: String*): GroupedData =
    df.groupBy(col1, cols: _*)
  def rollup(col1: String, cols: String*): GroupedData =
    df.rollup(col1, cols: _*)
  def cube(col1: String, cols: String*): GroupedData =
    df.cube(col1, cols: _*)
  
  def head(n: Int): Array[Row] =
    df.head(n)
  def head(): Row =
    df.head()
  def first(): Row =
    df.first()
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
  def take(n: Int): Array[Row] =
    df.take(n)
  def collect(): Array[Row] =
    df.collect()
  
  def count(): Long = df.count()
  def registerTempTable(tableName: String): Unit = df.registerTempTable(tableName)
  def write: DataFrameWriter = df.write
  def toJSON: RDD[String] = df.toJSON
  def inputFiles: Array[String] = df.inputFiles
}

class FieldRenamer[Schema, NewSchema](df: DataFrame) {
  def apply[S <: HList]()
    (implicit
      l: Generic.Aux[Schema, S],
      r: Generic.Aux[NewSchema, S]
    ): TypedFrame[NewSchema] =
     TypedFrame(df)
}
