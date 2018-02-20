package frameless

import scala.annotation.implicitNotFound

/** Types that can be remainded by Catalyst. */
@implicitNotFound("Cannot do integral operations on columns of type ${A}.")
trait CatalystIntegral[A]

object CatalystIntegral {
  private[this] val theInstance = new CatalystIntegral[Any] {}
  private[this] def of[A]: CatalystIntegral[A] = theInstance.asInstanceOf[CatalystIntegral[A]]

  implicit val framelessbigDecimalIntegral: CatalystIntegral[BigDecimal] = of[BigDecimal]
  implicit val framelessbyteIntegral      : CatalystIntegral[Byte]       = of[Byte]
  implicit val framelessdoubleIntegral    : CatalystIntegral[Double]     = of[Double]
  implicit val framelessintIntegral       : CatalystIntegral[Int]        = of[Int]
  implicit val framelesslongIntegral      : CatalystIntegral[Long]       = of[Long]
  implicit val framelessshortIntegral     : CatalystIntegral[Short]      = of[Short]
}
