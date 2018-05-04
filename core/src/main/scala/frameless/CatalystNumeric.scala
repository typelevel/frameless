package frameless

import scala.annotation.implicitNotFound

/** Types that can be added, subtracted and multiplied by Catalyst. */
@implicitNotFound("Cannot do numeric operations on columns of type ${A}.")
trait CatalystNumeric[A]

object CatalystNumeric {
  private[this] val theInstance = new CatalystNumeric[Any] {}
  private[this] def of[A]: CatalystNumeric[A] = theInstance.asInstanceOf[CatalystNumeric[A]]

  implicit val framelessbigDecimalNumeric: CatalystNumeric[BigDecimal] = of[BigDecimal]
  implicit val framelessbyteNumeric      : CatalystNumeric[Byte]       = of[Byte]
  implicit val framelessdoubleNumeric    : CatalystNumeric[Double]     = of[Double]
  implicit val framelessintNumeric       : CatalystNumeric[Int]        = of[Int]
  implicit val framelesslongNumeric      : CatalystNumeric[Long]       = of[Long]
  implicit val framelessshortNumeric     : CatalystNumeric[Short]      = of[Short]
  // implicit val framelessfloatNumeric     : CatalystNumeric[Float]      = of[Float]
}
