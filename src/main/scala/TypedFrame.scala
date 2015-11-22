import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class TypedFrame[Schema](df: DataFrame) {
  def toDF(): DataFrame = ???
  def toDF(colNames: String*): DataFrame = ???
  def schema: StructType = ???
  def dtypes: Array[(String, String)] = ???
  def columns: Array[String] = ???
  def printSchema(): Unit = ???
  def explain(extended: Boolean): Unit = ???
  def explain(): Unit = ???
  def isLocal: Boolean = ???
  def show(numRows: Int): Unit = ???
  def show(): Unit = ???
  def show(truncate: Boolean): Unit = ???
  def show(numRows: Int, truncate: Boolean): Unit = ???
  def na: DataFrameNaFunctions = ???
  def stat: DataFrameStatFunctions = ???
  def join(right: DataFrame): DataFrame = ???
  def join(right: DataFrame, usingColumn: String): DataFrame = ???
  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame = ???
  def join(right: DataFrame, joinExprs: Column): DataFrame = ???
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = ???
  def sort(sortCol: String, sortCols: String*): DataFrame = ???
  def sort(sortExprs: Column*): DataFrame = ???
  def orderBy(sortCol: String, sortCols: String*): DataFrame = ???
  def orderBy(sortExprs: Column*): DataFrame = ???
  def apply(colName: String): Column = ???
  def col(colName: String): Column = ???
  def as(alias: String): DataFrame = ???
  def as(alias: Symbol): DataFrame = ???
  def select(cols: Column*): DataFrame = ???
  def select(col: String, cols: String*): DataFrame = ???
  def selectExpr(exprs: String*): DataFrame = ???
  def filter(condition: Column): DataFrame = ???
  def filter(conditionExpr: String): DataFrame = ???
  def where(condition: Column): DataFrame = ???
  def where(conditionExpr: String): DataFrame = ???
  def groupBy(cols: Column*): GroupedData = ???
  def rollup(cols: Column*): GroupedData = ???
  def cube(cols: Column*): GroupedData = ???
  def groupBy(col1: String, cols: String*): GroupedData = ???
  def rollup(col1: String, cols: String*): GroupedData = ???
  def cube(col1: String, cols: String*): GroupedData = ???
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = ???
  def agg(exprs: Map[String, String]): DataFrame = ???
  def agg(exprs: java.util.Map[String, String]): DataFrame = ???
  def agg(expr: Column, exprs: Column*): DataFrame = ???
  def limit(n: Int): DataFrame = ???
  def unionAll(other: DataFrame): DataFrame = ???
  def intersect(other: DataFrame): DataFrame = ???
  def except(other: DataFrame): DataFrame = ???
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = ???
  def sample(withReplacement: Boolean, fraction: Double): DataFrame = ???
  def randomSplit(weights: Array[Double], seed: Long): Array[DataFrame] = ???
  def randomSplit(weights: Array[Double]): Array[DataFrame] = ???
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = ???
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B]): DataFrame = ???
  def withColumn(colName: String, col: Column): DataFrame = ???
  def withColumnRenamed(existingName: String, newName: String): DataFrame = ???
  def drop(colName: String): DataFrame = ???
  def drop(col: Column): DataFrame = ???
  def dropDuplicates(): DataFrame = ???
  def dropDuplicates(colNames: Seq[String]): DataFrame = ???
  def dropDuplicates(colNames: Array[String]): DataFrame = ???
  def describe(cols: String*): DataFrame = ???
  def head(n: Int): Array[Row] = ???
  def head(): Row = ???
  def first(): Row = ???
  def map[R: ClassTag](f: Row => R): RDD[R] = ???
  def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = ???
  def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = ???
  def foreach(f: Row => Unit): Unit = ???
  def foreachPartition(f: Iterator[Row] => Unit): Unit = ???
  def take(n: Int): Array[Row] = ???
  def collect(): Array[Row] = ???
  def collectAsList(): java.util.List[Row] = ???
  def count(): Long = ???
  def repartition(numPartitions: Int): DataFrame = ???
  def coalesce(numPartitions: Int): DataFrame = ???
  def distinct(): DataFrame = ???
  def persist(): this.type = ???
  def cache(): this.type = ???
  def persist(newLevel: StorageLevel): this.type = ???
  def unpersist(blocking: Boolean): this.type = ???
  def unpersist(): this.type = ???
  def registerTempTable(tableName: String): Unit = ???
  def write: DataFrameWriter = ???
  def toJSON: RDD[String] = ???
  def inputFiles: Array[String] = ???
  def toSchemaRDD: DataFrame = ???
  def createJDBCTable(url: String, table: String, allowExisting: Boolean): Unit = ???
  def insertIntoJDBC(url: String, table: String, overwrite: Boolean): Unit = ???
  def saveAsParquetFile(path: String): Unit = ???
  def saveAsTable(tableName: String): Unit = ???
  def saveAsTable(tableName: String, mode: SaveMode): Unit = ???
  def saveAsTable(tableName: String, source: String): Unit = ???
  def saveAsTable(tableName: String, source: String, mode: SaveMode): Unit = ???
  def save(path: String): Unit = ???
  def save(path: String, mode: SaveMode): Unit = ???
  def save(path: String, source: String): Unit = ???
  def save(path: String, source: String, mode: SaveMode): Unit = ???
  def insertInto(tableName: String, overwrite: Boolean): Unit = ???
  def insertInto(tableName: String): Unit = ???
}
