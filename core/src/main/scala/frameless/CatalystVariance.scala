package frameless

import scala.annotation.implicitNotFound

/**
  * Spark's variance and stddev functions always return Double
  */
@implicitNotFound("Cannot compute variance on type ${A}.")
trait CatalystVariance[A]

object CatalystVariance {
  private[this] val theInstance = new CatalystVariance[Any] {}
  private[this] def of[A]: CatalystVariance[A] = theInstance.asInstanceOf[CatalystVariance[A]]

  implicit val framelessIntVariance       : CatalystVariance[Int]        = of[Int]
  implicit val framelessLongVariance      : CatalystVariance[Long]       = of[Long]
  implicit val framelessShortVariance     : CatalystVariance[Short]      = of[Short]
  implicit val framelessBigDecimalVariance: CatalystVariance[BigDecimal] = of[BigDecimal]
  implicit val framelessDoubleVariance    : CatalystVariance[Double]     = of[Double]
}
