package frameless

trait CatalystCast[A, B]

object CatalystCast {
  implicit object intToDouble extends CatalystCast[Int, Double]
}
