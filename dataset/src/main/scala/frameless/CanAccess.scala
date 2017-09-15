package frameless

/** `CanAccess[_, A with B]` indicates that in this context it is possible to
 *  access columns from both table `A` and table `B`. The first type parameter
 * is a dummy argument used for type inference.
 */
sealed trait CanAccess[-T, X]

object CanAccess {
  private[this] val theInstance = new CanAccess[Nothing, Nothing] {}
  private[frameless] def localCanAccessInstance[X]: CanAccess[Any, X] = theInstance.asInstanceOf[CanAccess[Any, X]]

  implicit def globalCanAccessInstance[X] = theInstance.asInstanceOf[CanAccess[X, X]]
  // The trick works as follows: `(df: TypedDataset[T]).col('a)` looks for a
  // CanAccess[T, T] which is always available thanks to the `globalInstance`
  // implicit defined above. Expression for joins (and other multi dataset
  // operations) take an `implicit a: CanAccess[Any, U with T] =>` closure.
  // Because the first (dummy) type parameter of `CanAccess` is contravariant,
  // the locally defined implicit will always be preferred over
  // `globalInstance`, which implements the desired behavior.
}
