package frameless

import scala.annotation.implicitNotFound

/** Spark does NaN check only for these types */
@implicitNotFound("Columns of type ${A} cannot be NaN.")
trait CatalystNaN[A]

object CatalystNaN {
  private[this] val theInstance = new CatalystNaN[Any] {}
  private[this] def of[A]: CatalystNaN[A] = theInstance.asInstanceOf[CatalystNaN[A]]

  implicit val framelessFloatNaN     : CatalystNaN[Float]      = of[Float]
  implicit val framelessDoubleNaN    : CatalystNaN[Double]     = of[Double]
}

