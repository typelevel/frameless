package frameless

import scala.annotation.implicitNotFound

/** Spark does not return always the same type as the input was for example abs
  */
@implicitNotFound("Cannot compute on type ${In}.")
trait CatalystNumericWithJavaBigDecimal[In, Out]

object CatalystNumericWithJavaBigDecimal {
  private[this] val theInstance = new CatalystNumericWithJavaBigDecimal[Any, Any] {}
  private[this] def of[In, Out]: CatalystNumericWithJavaBigDecimal[In, Out] = theInstance.asInstanceOf[CatalystNumericWithJavaBigDecimal[In, Out]]

  implicit val framelessAbsoluteBigDecimal: CatalystNumericWithJavaBigDecimal[BigDecimal, java.math.BigDecimal]  = of[BigDecimal, java.math.BigDecimal]
  implicit val framelessAbsoluteDouble    : CatalystNumericWithJavaBigDecimal[Double, Double]                    = of[Double, Double]
  implicit val framelessAbsoluteInt       : CatalystNumericWithJavaBigDecimal[Int, Int]                          = of[Int, Int]
  implicit val framelessAbsoluteLong      : CatalystNumericWithJavaBigDecimal[Long, Long]                        = of[Long, Long]
  implicit val framelessAbsoluteShort     : CatalystNumericWithJavaBigDecimal[Short, Short]                      = of[Short, Short]
}