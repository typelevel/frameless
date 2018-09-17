package frameless

import scala.annotation.implicitNotFound

/** Spark does not return always the same type on abs as the input was
  */
@implicitNotFound("Cannot compute bround on type ${In}.")
trait CatalystBround[In, Out]

object CatalystBround {
  private[this] val theInstance = new CatalystBround[Any, Any] {}
  private[this] def of[In, Out]: CatalystBround[In, Out] = theInstance.asInstanceOf[CatalystBround[In, Out]]

  implicit val framelessAbsoluteBigDecimal: CatalystBround[BigDecimal, java.math.BigDecimal]  = of[BigDecimal, java.math.BigDecimal]
  implicit val framelessAbsoluteDouble    : CatalystBround[Double, Double]                    = of[Double, Double]
  implicit val framelessAbsoluteInt       : CatalystBround[Int, Int]                          = of[Int, Int]
  implicit val framelessAbsoluteLong      : CatalystBround[Long, Long]                        = of[Long, Long]
  implicit val framelessAbsoluteShort     : CatalystBround[Short, Short]                      = of[Short, Short]
}