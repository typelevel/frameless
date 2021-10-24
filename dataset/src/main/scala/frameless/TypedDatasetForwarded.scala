package frameless

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/** This trait implements [[TypedDataset]] methods that have the same signature
  * than their `Dataset` equivalent. Each method simply forwards the call to the
  * underlying `Dataset`.
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
trait TypedDatasetForwarded[T] { self: TypedDataset[T] =>

  override def toString: String =
    dataset.toString

  /**
    * Returns a `SparkSession` from this [[TypedDataset]].
    */
  def sparkSession: SparkSession =
    dataset.sparkSession

  /**
    * Returns a `SQLContext` from this [[TypedDataset]].
    */
  def sqlContext: SQLContext =
    dataset.sqlContext

  /**
    * Returns the schema of this Dataset.
    *
    * apache/spark
    */
  def schema: StructType =
    dataset.schema

  /** Prints the schema of the underlying `Dataset` to the console in a nice tree format.
    *
    * apache/spark
   */
  def printSchema(): Unit =
    dataset.printSchema()

  /** Prints the plans (logical and physical) to the console for debugging purposes.
    *
    * apache/spark
   */
  def explain(extended: Boolean = false): Unit =
    dataset.explain(extended)

  /**
    * Returns a `QueryExecution` from this [[TypedDataset]].
    *
    * It is the primary workflow for executing relational queries using Spark.  Designed to allow easy
    * access to the intermediate phases of query execution for developers.
    *
    * apache/spark
    */
  def queryExecution: QueryExecution =
    dataset.queryExecution

  /** Converts this strongly typed collection of data to generic Dataframe.  In contrast to the
    * strongly typed objects that Dataset operations work on, a Dataframe returns generic Row
    * objects that allow fields to be accessed by ordinal or name.
    *
    * apache/spark
    */
  def toDF(): DataFrame =
    dataset.toDF()

  /** Converts this [[TypedDataset]] to an RDD.
    *
    * apache/spark
    */
  def rdd: RDD[T] =
    dataset.rdd

  /** Returns a new [[TypedDataset]] that has exactly `numPartitions` partitions.
    *
    * apache/spark
    */
  def repartition(numPartitions: Int): TypedDataset[T] =
    TypedDataset.create(dataset.repartition(numPartitions))


  /**
    * Get the [[TypedDataset]]'s current storage level, or StorageLevel.NONE if not persisted.
    *
    * apache/spark
    */
  def storageLevel(): StorageLevel =
    dataset.storageLevel

  /**
    * Returns the content of the [[TypedDataset]] as a Dataset of JSON strings.
    *
    * apache/spark
    */
  def toJSON: TypedDataset[String] =
    TypedDataset.create(dataset.toJSON)

  /**
    * Interface for saving the content of the non-streaming [[TypedDataset]] out into external storage.
    *
    * apache/spark
    */
  def write: DataFrameWriter[T] =
    dataset.write

  /**
    * Interface for saving the content of the streaming Dataset out into external storage.
    *
    * apache/spark
    */
  def writeStream: DataStreamWriter[T] =
    dataset.writeStream
    
  /** Returns a new [[TypedDataset]] that has exactly `numPartitions` partitions.
    * Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g.
    * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
    * the 100 new partitions will claim 10 of the current partitions.
    *
    * apache/spark
    */
  def coalesce(numPartitions: Int): TypedDataset[T] =
    TypedDataset.create(dataset.coalesce(numPartitions))

  /**
    * Returns an `Array` that contains all column names in this [[TypedDataset]].
    */
  def columns: Array[String] =
    dataset.columns

  /** Concise syntax for chaining custom transformations.
    *
    * apache/spark
    */
  def transform[U](t: TypedDataset[T] => TypedDataset[U]): TypedDataset[U] =
    t(this)

  /** Returns a new Dataset by taking the first `n` rows. The difference between this function
    * and `head` is that `head` is an action and returns an array (by triggering query execution)
    * while `limit` returns a new Dataset.
    *
    * apache/spark
    */
  def limit(n: Int): TypedDataset[T] =
    TypedDataset.create(dataset.limit(n))

  /** Returns a new [[TypedDataset]] by sampling a fraction of records.
    *
    * apache/spark
    */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = Random.nextLong()): TypedDataset[T] =
    TypedDataset.create(dataset.sample(withReplacement, fraction, seed))

  /** Returns a new [[TypedDataset]] that contains only the unique elements of this [[TypedDataset]].
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def distinct: TypedDataset[T] =
    TypedDataset.create(dataset.distinct())

  /**
    * Returns a best-effort snapshot of the files that compose this [[TypedDataset]]. This method simply
    * asks each constituent BaseRelation for its respective files and takes the union of all results.
    * Depending on the source relations, this may not find all input files. Duplicates are removed.
    *
    * apache/spark
    */

  def inputFiles: Array[String] =
    dataset.inputFiles

  /**
    * Returns true if the `collect` and `take` methods can be run locally
    * (without any Spark executors).
    *
    * apache/spark
    */
  def isLocal: Boolean =
    dataset.isLocal

  /**
    * Returns true if this [[TypedDataset]] contains one or more sources that continuously
    * return data as it arrives. A [[TypedDataset]] that reads data from a streaming source
    * must be executed as a `StreamingQuery` using the `start()` method in
    * `DataStreamWriter`. Methods that return a single answer, e.g. `count()` or
    * `collect()`, will throw an `AnalysisException` when there is a streaming
    * source present.
    *
    * apache/spark
    */
  def isStreaming: Boolean =
    dataset.isStreaming

  /** Returns a new [[TypedDataset]] that contains only the elements of this [[TypedDataset]] that are also
    * present in `other`.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def intersect(other: TypedDataset[T]): TypedDataset[T] =
    TypedDataset.create(dataset.intersect(other.dataset))

  /**
    * Randomly splits this [[TypedDataset]] with the provided weights.
    * Weights for splits, will be normalized if they don't sum to 1.
    *
    * apache/spark
    */
  // $COVERAGE-OFF$ We can not test this method because it is non-deterministic.
  def randomSplit(weights: Array[Double]): Array[TypedDataset[T]] =
    dataset.randomSplit(weights).map(TypedDataset.create[T])
  // $COVERAGE-ON$

  /**
    * Randomly splits this [[TypedDataset]] with the provided weights.
    * Weights for splits, will be normalized if they don't sum to 1.
    *
    * apache/spark
    */
  def randomSplit(weights: Array[Double], seed: Long): Array[TypedDataset[T]] =
    dataset.randomSplit(weights, seed).map(TypedDataset.create[T])

  /**
    * Returns a Java list that contains randomly split [[TypedDataset]] with the provided weights.
    * Weights for splits, will be normalized if they don't sum to 1.
    *
    * apache/spark
    */
  def randomSplitAsList(weights: Array[Double], seed: Long): util.List[TypedDataset[T]] = {
    val values = randomSplit(weights, seed)
    java.util.Arrays.asList(values: _*)
  }


  /** Returns a new Dataset containing rows in this Dataset but not in another Dataset.
    * This is equivalent to `EXCEPT` in SQL.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def except(other: TypedDataset[T]): TypedDataset[T] =
    TypedDataset.create(dataset.except(other.dataset))

  /** Persist this [[TypedDataset]] with the default storage level (`MEMORY_AND_DISK`).
    *
    * apache/spark
    */
  def cache(): TypedDataset[T] =
    TypedDataset.create(dataset.cache())

  /** Persist this [[TypedDataset]] with the given storage level.
    * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
    *   `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.
    *
    * apache/spark
    */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): TypedDataset[T] =
    TypedDataset.create(dataset.persist(newLevel))

  /** Mark the [[TypedDataset]] as non-persistent, and remove all blocks for it from memory and disk.
    * @param blocking Whether to block until all blocks are deleted.
    *
    * apache/spark
    */
  def unpersist(blocking: Boolean = false): TypedDataset[T] =
    TypedDataset.create(dataset.unpersist(blocking))

  // $COVERAGE-OFF$ We do not test deprecated method since forwarded methods are tested.
  @deprecated("deserialized methods have moved to a separate section to highlight their runtime overhead", "0.4.0")
  def map[U: TypedEncoder](func: T => U): TypedDataset[U] =
    deserialized.map(func)

  @deprecated("deserialized methods have moved to a separate section to highlight their runtime overhead", "0.4.0")
  def mapPartitions[U: TypedEncoder](func: Iterator[T] => Iterator[U]): TypedDataset[U] =
    deserialized.mapPartitions(func)

  @deprecated("deserialized methods have moved to a separate section to highlight their runtime overhead", "0.4.0")
  def flatMap[U: TypedEncoder](func: T => TraversableOnce[U]): TypedDataset[U] =
    deserialized.flatMap(func)

  @deprecated("deserialized methods have moved to a separate section to highlight their runtime overhead", "0.4.0")
  def filter(func: T => Boolean): TypedDataset[T] =
    deserialized.filter(func)

  @deprecated("deserialized methods have moved to a separate section to highlight their runtime overhead", "0.4.0")
  def reduceOption[F[_]: SparkDelay](func: (T, T) => T): F[Option[T]] =
    deserialized.reduceOption(func)
  // $COVERAGE-ON$

  /** Methods on `TypedDataset[T]` that go through a full serialization and
    * deserialization of `T`, and execute outside of the Catalyst runtime.
    *
    * @example The correct way to do a projection on a single column is to
    *          use the `select` method as follows:
    *
    *          {{{
    *           ds: TypedDataset[(String, String, String)] -> ds.select(ds('_2)).run()
    *          }}}
    *
    *          Spark provides an alternative way to obtain the same resulting `Dataset`,
    *          using the `map` method:
    *
    *          {{{
    *           ds: TypedDataset[(String, String, String)] -> ds.deserialized.map(_._2).run()
    *          }}}
    *
    *          This second approach is however substantially slower than the first one,
    *          and should be avoided as possible. Indeed, under the hood this `map` will
    *          deserialize the entire `Tuple3` to an full JVM object, call the apply
    *          method of the `_._2` closure on it, and serialize the resulting String back
    *          to its Catalyst representation.
    */
  object deserialized {
    /** Returns a new [[TypedDataset]] that contains the result of applying `func` to each element.
      *
      * apache/spark
      */
    def map[U: TypedEncoder](func: T => U): TypedDataset[U] =
      TypedDataset.create(self.dataset.map(func)(TypedExpressionEncoder[U]))

    /** Returns a new [[TypedDataset]] that contains the result of applying `func` to each partition.
      *
      * apache/spark
      */
    def mapPartitions[U: TypedEncoder](func: Iterator[T] => Iterator[U]): TypedDataset[U] =
      TypedDataset.create(self.dataset.mapPartitions(func)(TypedExpressionEncoder[U]))

    /** Returns a new [[TypedDataset]] by first applying a function to all elements of this [[TypedDataset]],
      * and then flattening the results.
      *
      * apache/spark
      */
    def flatMap[U: TypedEncoder](func: T => TraversableOnce[U]): TypedDataset[U] =
      TypedDataset.create(self.dataset.flatMap(func)(TypedExpressionEncoder[U]))

    /** Returns a new [[TypedDataset]] that only contains elements where `func` returns `true`.
      *
      * apache/spark
      */
    def filter(func: T => Boolean): TypedDataset[T] =
      TypedDataset.create(self.dataset.filter(func))

    /** Optionally reduces the elements of this [[TypedDataset]] using the specified binary function. The given
      * `func` must be commutative and associative or the result may be non-deterministic.
      *
      * Differs from `Dataset#reduce` by wrapping its result into an `Option` and an effect-suspending `F`.
      */
    def reduceOption[F[_]](func: (T, T) => T)(implicit F: SparkDelay[F]): F[Option[T]] =
      F.delay {
        try {
          Option(self.dataset.reduce(func))
        } catch {
          case _: UnsupportedOperationException => None
        }
      }(self.dataset.sparkSession)
  }
}
