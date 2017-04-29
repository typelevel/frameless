package frameless

/** An Injection[A, B] is a reversible function from A to B.
  *
  * Must obey `forAll { a: A => invert(apply(a)) == a }`.
  */
trait Injection[A, B] extends Serializable {
  def apply(a: A): B
  def invert(b: B): A
}

object Injection {
  def apply[A, B](f: A => B, g: B => A): Injection[A, B] = new Injection[A, B] {
    def apply(a: A): B = f(a)
    def invert(b: B): A = g(b)
  }
}
