package frameless

trait Isomorphism[A, B] {
  def map(a: A): B
  def comap(b: B): A
}

object Isomorphism {
  def apply[A,B](f: A => B, g: B => A): Isomorphism[A,B] = new Isomorphism[A,B] {
    def map(a: A): B = f(a)
    def comap(b: B): A = g(b)
  }
}
