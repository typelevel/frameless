package frameless
package ops

package object unoptimized {
  /** Contains methods on `TypedDataset[T]` that use the entire `T`. If only a small fraction of the fields
    * of `T` are used, these method will not include any projection optimizations.
    *
    * @example Even though only one column is ever used (_2), the following code will require reading all the columns
    *          of the parquet file.
    * {{{
    *  (parquet file) -> ds: TypedDataset[(String, String, String)] -> ds.map(_._2).show().run()
    * }}}
    */
  implicit class UnoptimizedDatasetOps[T: TypedEncoder](self: TypedDataset[T]) {
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
      * Differs from `Dataset#reduce` by wrapping it's result into an `Option` and a [[Job]].
      */
    def reduceOption(func: (T, T) => T): Job[Option[T]] =
      Job {
        try {
          Option(self.dataset.reduce(func))
        } catch {
          case _: UnsupportedOperationException => None
        }
      }(self.dataset.sparkSession.sparkContext)
  }
}
