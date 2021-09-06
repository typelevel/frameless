package frameless

/** Compute the intersection of two types:
  *
  * - With[A, A] = A
  * - With[A, B] = A with B (when A != B)
  *
  * This type function is needed to prevent IDEs from infering large types
  * with shape `A with A with ... with A`. These types could be confusing for
  * both end users and IDE's type checkers.
  */
trait With[A, B] { type Out }

object With extends LowPrioWith {
  implicit def combine[A, B]: Aux[A, B, A with B] = of[A, B, A with B]
}

private[frameless] sealed trait LowPrioWith {
  type Aux[A, B, W] = With[A, B] { type Out = W }

  protected[this] val theInstance = new With[Any, Any] {}

  protected[this] def of[A, B, W]: With[A, B] { type Out = W } =
    theInstance.asInstanceOf[Aux[A, B, W]]

  implicit def identity[T]: Aux[T, T, T] = of[T, T, T]
}
