package frameless.ops

import frameless.{TypedColumn, TypedDataset, TypedEncoder}

/**
 * Collection of forwarding functions that optionally provide a reference to the incoming dataset for chaining of joins
 * @param ds the dataset on which .join(other) was called
 * @param other the dataset to which ds is joined
 * @tparam T the type of ds
 * @tparam U the type of other
 */
case class ChainedJoinOps[T, U](ds: TypedDataset[T], other: TypedDataset[U]) {
  /** Computes the right outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def right(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean])(implicit e: TypedEncoder[(Option[T], U)]): TypedDataset[(Option[T], U)] =
    ds.joinRight(other)(conditionF(ds))

  /** Computes the right outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def right(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean])(implicit e: TypedEncoder[(Option[T], U)]): TypedDataset[(Option[T], U)] =
    ds.joinRight(other)(conditionF(ds, other))

  /** Computes the right outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def right(condition: TypedColumn[T with U, Boolean])(implicit e: TypedEncoder[(Option[T], U)]): TypedDataset[(Option[T], U)] =
    ds.joinRight(other)(condition)

  /** Computes the cartesian project of `this` `Dataset` with the `other` `Dataset` */
  def cross() // here for completeness
                  (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
    ds.joinCross(other)

  /** Computes the full outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def full(condition: TypedColumn[T with U, Boolean])
                 (implicit e: TypedEncoder[(Option[T], Option[U])]): TypedDataset[(Option[T], Option[U])] =
    ds.joinFull(other)(condition)

  /** Computes the full outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def full(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean])
             (implicit e: TypedEncoder[(Option[T], Option[U])]): TypedDataset[(Option[T], Option[U])] =
    ds.joinFull(other)(conditionF(ds))

  /** Computes the full outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def full(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean])
          (implicit e: TypedEncoder[(Option[T], Option[U])]): TypedDataset[(Option[T], Option[U])] =
    ds.joinFull(other)(conditionF(ds, other))

  /** Computes the inner join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def inner(condition: TypedColumn[T with U, Boolean])
                  (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
    ds.joinInner(other)(condition)

  /** Computes the inner join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def inner(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean])
              (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
    ds.joinInner(other)(conditionF(ds))

  /** Computes the inner join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def inner(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean])
           (implicit e: TypedEncoder[(T, U)]): TypedDataset[(T, U)] =
    ds.joinInner(other)(conditionF(ds, other))

  /** Computes the left outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def left(condition: TypedColumn[T with U, Boolean])
                 (implicit e: TypedEncoder[(T, Option[U])]): TypedDataset[(T, Option[U])] =
    ds.joinLeft(other)(condition)

  /** Computes the left outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def left(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean])
          (implicit e: TypedEncoder[(T, Option[U])]): TypedDataset[(T, Option[U])] =
    ds.joinLeft(other)(conditionF(ds))

  /** Computes the left outer join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def left(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean])
          (implicit e: TypedEncoder[(T, Option[U])]): TypedDataset[(T, Option[U])] =
    ds.joinLeft(other)(conditionF(ds,other))

  /** Computes the left semi join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def leftSemi(condition: TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftSemi(other)(condition)

  /** Computes the left semi join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def leftSemi(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftSemi(other)(conditionF(ds))

  /** Computes the left semi join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def leftSemi(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftSemi(other)(conditionF(ds, other))

  /** Computes the left anti join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   */
  def leftAnti(condition: TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftAnti(other)(condition)

  /** Computes the left anti join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current dataset in the chain to the conditionF allowing you access to this TypedDataset's columns
   */
  def leftAnti(conditionF: TypedDataset[T] => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftAnti(other)(conditionF(ds))

  /** Computes the left anti join of `this` `Dataset` with the `other` `Dataset`,
   * returning a `Tuple2` for each pair where condition evaluates to true.
   *
   * This version passes in the current and joined datasets in the chain to the conditionF allowing you access to this TypedDataset's columns and the joins
   */
  def leftAnti(conditionF: (TypedDataset[T], TypedDataset[U]) => TypedColumn[T with U, Boolean]): TypedDataset[T] =
    ds.joinLeftAnti(other)(conditionF(ds, other))

}
