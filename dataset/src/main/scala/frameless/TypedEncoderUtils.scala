package frameless

/** Utils for Spark interop */
object TypedEncoderUtils {
  def mkVector[A](xs: Array[A]): Vector[A] = Vector(xs: _*)
}
