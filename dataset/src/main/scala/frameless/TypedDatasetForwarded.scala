package frameless

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
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
    new TypedDataset(dataset.repartition(numPartitions))

  /** Returns a new [[TypedDataset]] that has exactly `numPartitions` partitions.
    * Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g.
    * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
    * the 100 new partitions will claim 10 of the current partitions.
    *
    * apache/spark
    */
  def coalesce(numPartitions: Int): TypedDataset[T] =
    new TypedDataset(dataset.coalesce(numPartitions))

  /** Concise syntax for chaining custom transformations.
    *
    * apache/spark
    */
  def transform[U](t: TypedDataset[T] => TypedDataset[U]): TypedDataset[U] =
    t(this)

  /** Returns a new [[TypedDataset]] that only contains elements where `func` returns `true`.
    *
    * apache/spark
    */
  def filter(func: T => Boolean): TypedDataset[T] =
    new TypedDataset(dataset.filter(func))

  /** Returns a new [[TypedDataset]] that contains the result of applying `func` to each element.
    *
    * apache/spark
    */
  def map[U: TypedEncoder](func: T => U): TypedDataset[U] =
    new TypedDataset(dataset.map(func)(TypedExpressionEncoder[U]))

  /** Returns a new [[TypedDataset]] that contains the result of applying `func` to each partition.
    *
    * apache/spark
    */
  def mapPartitions[U: TypedEncoder](func: Iterator[T] => Iterator[U]): TypedDataset[U] =
    new TypedDataset(dataset.mapPartitions(func)(TypedExpressionEncoder[U]))

  /** Returns a new [[TypedDataset]] by first applying a function to all elements of this [[TypedDataset]],
    * and then flattening the results.
    *
    * apache/spark
    */
  def flatMap[U: TypedEncoder](func: T => TraversableOnce[U]): TypedDataset[U] =
    new TypedDataset(dataset.flatMap(func)(TypedExpressionEncoder[U]))

  /** Returns a new [[TypedDataset]] by sampling a fraction of records.
    *
    * apache/spark
    */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = Random.nextLong): TypedDataset[T] =
    new TypedDataset(dataset.sample(withReplacement, fraction, seed))

  /** Returns a new [[TypedDataset]] that contains only the unique elements of this [[TypedDataset]].
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def distinct: TypedDataset[T] =
    new TypedDataset(dataset.distinct)

  /** Returns a new [[TypedDataset]] that contains only the elements of this [[TypedDataset]] that are also
    * present in `other`.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def intersect(other: TypedDataset[T]): TypedDataset[T] =
    new TypedDataset(dataset.intersect(other.dataset))

  /** Returns a new [[TypedDataset]] that contains the elements of both this and the `other` [[TypedDataset]]
    * combined.
    *
    * Note that, this function is not a typical set union operation, in that it does not eliminate
    * duplicate items.  As such, it is analogous to `UNION ALL` in SQL.
    *
    * apache/spark
    */
  def union(other: TypedDataset[T]): TypedDataset[T] =
    new TypedDataset(dataset.union(other.dataset))

  /** Returns a new [[TypedDataset]] where any elements present in `other` have been removed.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def subtract(other: TypedDataset[T]): TypedDataset[T] =
    new TypedDataset(dataset.subtract(other.dataset))

  /** Persist this [[TypedDataset]] with the default storage level (`MEMORY_AND_DISK`).
    *
    * apache/spark
    */
  def cache(): TypedDataset[T] =
    new TypedDataset(dataset.cache())

  /** Persist this [[TypedDataset]] with the given storage level.
    * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
    *   `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.
    *
    * apache/spark
    */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): TypedDataset[T] =
    new TypedDataset(dataset.persist(newLevel))

  /** Mark the [[TypedDataset]] as non-persistent, and remove all blocks for it from memory and disk.
    * @param blocking Whether to block until all blocks are deleted.
    *
    * apache/spark
    */
  def unpersist(blocking: Boolean = false): TypedDataset[T] =
    new TypedDataset(dataset.unpersist(blocking))
}
