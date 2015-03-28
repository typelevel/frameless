package frameless

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SaveMode, SQLContext }
import org.apache.spark.storage.StorageLevel

import scala.reflect.runtime.universe.TypeTag

import shapeless.{ Generic, HList }
import shapeless.ops.hlist.Prepend

/** Wrapper around [[org.apache.spark.sql.DataFrame]] using [[shapeless.HList]]s to track schema.
  *
  * All heavy-lifting is still being done by the backing DataFrame so this API will more or less
  * be 1-to-1 with that of the DataFrame's.
  */
abstract class DataSheet[L <: HList] {
  val dataFrame: DataFrame

  def as(alias: Symbol): DataSheet[L] = DataSheet(dataFrame.as(alias))

  def as(alias: String): DataSheet[L] = DataSheet(dataFrame.as(alias))

  def cache(): this.type = {
    dataFrame.cache()
    this
  }

  def count(): Long = dataFrame.count()

  def distinct: DataSheet[L] = DataSheet(dataFrame.distinct)

  def except(other: DataSheet[L]): DataSheet[L] =
    DataSheet(dataFrame.except(other.dataFrame))

  def explain(): Unit = dataFrame.explain()

  def explain(extended: Boolean): Unit = dataFrame.explain(extended)

  def intersect(other: DataSheet[L]): DataSheet[L] =
    DataSheet(dataFrame.intersect(other.dataFrame))

  def isLocal: Boolean = dataFrame.isLocal

  def join[M <: HList, Out <: HList](right: DataSheet[M])(implicit P: Prepend.Aux[L, M, Out]): DataSheet[Out] =
    DataSheet(dataFrame.join(right.dataFrame))

  def limit(n: Int): DataSheet[L] = DataSheet(dataFrame.limit(n))

  def persist(newLevel: StorageLevel): this.type = {
    dataFrame.persist(newLevel)
    this
  }

  def persist(): this.type = {
    dataFrame.persist()
    this
  }

  def printSchema(): Unit = dataFrame.printSchema()

  val queryExecution = dataFrame.queryExecution

  def registerTempTable(tableName: String): Unit = dataFrame.registerTempTable(tableName)

  def repartition(numPartitions: Int): DataSheet[L] = DataSheet(dataFrame.repartition(numPartitions))

  def sample(withReplacement: Boolean, fraction: Double): DataSheet[L] =
    DataSheet(dataFrame.sample(withReplacement, fraction))

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataSheet[L] =
    DataSheet(dataFrame.sample(withReplacement, fraction, seed))

  def save(source: String, mode: SaveMode, options: Map[String, String]): Unit = dataFrame.save(source, mode, options)

  def save(path: String, source: String, mode: SaveMode): Unit = dataFrame.save(path, source, mode)

  def save(path: String, source: String): Unit = dataFrame.save(path, source)

  def save(path: String, mode: SaveMode): Unit = dataFrame.save(path, mode)

  def save(path: String): Unit = dataFrame.save(path)

  def saveAsParquetFile(path: String): Unit = dataFrame.saveAsParquetFile(path)

  def saveAsTable(tableName: String, source: String, mode: SaveMode, options: Map[String, String]): Unit =
    dataFrame.saveAsTable(tableName, source, mode, options)

  def saveAsTable(tableName: String, source: String, mode: SaveMode): Unit =
    dataFrame.saveAsTable(tableName, source, mode)

  def saveAsTable(tableName: String, source: String): Unit =
    dataFrame.saveAsTable(tableName, source)

  def saveAsTable(tableName: String, mode: SaveMode): Unit =
    dataFrame.saveAsTable(tableName, mode)

  def saveAsTable(tableName: String): Unit =
    dataFrame.saveAsTable(tableName)

  def show(): Unit = dataFrame.show()

  def show(numRows: Int): Unit = dataFrame.show(numRows)

  val sqlContext: SQLContext = dataFrame.sqlContext

  override def toString(): String = s"DataSheet:\n${dataFrame.toString}"

  def unionAll(other: DataSheet[L]): DataSheet[L] =
    DataSheet(dataFrame.unionAll(other.dataFrame))

  def unpersist(): this.type = {
    dataFrame.unpersist()
    this
  }

  def unpersist(blocking: Boolean): this.type = {
    dataFrame.unpersist(blocking)
    this
  }

  /////////////////////////

  def collect[P <: Product](implicit Gen: Generic.Aux[P, L]): Array[P] = ???

  def collectAsList[P <: Product](implicit Gen: Generic.Aux[P, L]): java.util.List[P] = ???

  def head[P <: Product](implicit Gen: Generic.Aux[P, L]): P = ???

  def head[P <: Product](n: Int)(implicit Gen: Generic.Aux[P, L]): Array[P] = ???

  def rdd[P <: Product](implicit Gen: Generic.Aux[P, L]): RDD[P] = ???

  def take[P <: Product](n: Int)(implicit Gen: Generic.Aux[P, L]): Array[P] = ???
}

object DataSheet {
  private def apply[L <: HList](_dataFrame: DataFrame): DataSheet[L] =
    new DataSheet[L] { val dataFrame = _dataFrame }

  def fromRdd[P <: Product : TypeTag, L <: HList](rdd: RDD[P])(implicit Gen: Generic.Aux[P, L]): DataSheet[L] =
    DataSheet(new SQLContext(rdd.sparkContext).implicits.rddToDataFrameHolder(rdd).toDF())
}
