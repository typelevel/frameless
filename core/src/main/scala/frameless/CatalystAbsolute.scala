package frameless

import scala.annotation.implicitNotFound

/** Spark does not return always the same type on abs as the input was
  */
@implicitNotFound("Cannot compute absolute on type ${In}.")
trait CatalystAbsolute[In, Out]

object CatalystAbsolute {
  private[this] val theInstance = new CatalystAbsolute[Any, Any] {}
  private[this] def of[In, Out]: CatalystAbsolute[In, Out] = theInstance.asInstanceOf[CatalystAbsolute[In, Out]]

  implicit val framelessAbsoluteBigDecimal: CatalystAbsolute[BigDecimal, java.math.BigDecimal]  = of[BigDecimal, java.math.BigDecimal]
  implicit val framelessAbsoluteDouble    : CatalystAbsolute[Double, Double]                    = of[Double, Double]
  implicit val framelessAbsoluteInt       : CatalystAbsolute[Int, Int]                          = of[Int, Int]
  implicit val framelessAbsoluteLong      : CatalystAbsolute[Long, Long]                        = of[Long, Long]
  implicit val framelessAbsoluteShort     : CatalystAbsolute[Short, Short]                      = of[Short, Short]
}