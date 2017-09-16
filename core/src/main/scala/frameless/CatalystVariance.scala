package frameless

import scala.annotation.implicitNotFound

/**
  * Spark's variance and stddev functions always return Double
  */
@implicitNotFound("Cannot compute variance on type ${A}.")
trait CatalystVariance[A]

object CatalystVariance {
  implicit val intVariance: CatalystVariance[Int] = new CatalystVariance[Int] {}
  implicit val longVariance: CatalystVariance[Long] = new CatalystVariance[Long] {}
  implicit val shortVariance: CatalystVariance[Short] = new CatalystVariance[Short] {}
  implicit val bigDecimalVariance: CatalystVariance[BigDecimal] = new CatalystVariance[BigDecimal] {}
  implicit val doubleVariance: CatalystVariance[Double] = new CatalystVariance[Double] {}
}
