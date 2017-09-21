package frameless

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
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

  /** Returns a new [[TypedDataset]] that has exactly `numPartitions` partitions.
    * Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g.
    * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
    * the 100 new partitions will claim 10 of the current partitions.
    *
    * apache/spark
    */
  def coalesce(numPartitions: Int): TypedDataset[T] =
    TypedDataset.create(dataset.coalesce(numPartitions))

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
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = Random.nextLong): TypedDataset[T] =
    TypedDataset.create(dataset.sample(withReplacement, fraction, seed))

  /** Returns a new [[TypedDataset]] that contains only the unique elements of this [[TypedDataset]].
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function defined on `T`.
    *
    * apache/spark
    */
  def distinct: TypedDataset[T] =
    TypedDataset.create(dataset.distinct)

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

  /** Returns a new [[TypedDataset]] that contains the elements of both this and the `other` [[TypedDataset]]
    * combined.
    *
    * Note that, this function is not a typical set union operation, in that it does not eliminate
    * duplicate items.  As such, it is analogous to `UNION ALL` in SQL.
    *
    * apache/spark
    */
  def union(other: TypedDataset[T]): TypedDataset[T] =
    TypedDataset.create(dataset.union(other.dataset))

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
      * Differs from `Dataset#reduce` by wrapping it's result into an `Option` and an effect-suspending `F`.
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