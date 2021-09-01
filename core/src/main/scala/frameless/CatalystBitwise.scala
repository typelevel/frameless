package frameless

import scala.annotation.implicitNotFound

/**
 * Types that can be bitwise ORed, ANDed, or XORed by Catalyst.
 * Note that Catalyst requires that when performing bitwise operations between columns
 * the two types must be the same so in some cases casting is necessary.
 */
@implicitNotFound("Cannot do bitwise operations on columns of type ${A}.")
trait CatalystBitwise[A] extends CatalystNumeric[A]

object CatalystBitwise {
  private[this] val theInstance = new CatalystBitwise[Any] {}
  private[this] def of[A]: CatalystBitwise[A] = theInstance.asInstanceOf[CatalystBitwise[A]]

  implicit val framelessbyteBitwise: CatalystBitwise[Byte] = of[Byte]
  implicit val framelessshortBitwise: CatalystBitwise[Short] = of[Short]
  implicit val framelessintBitwise: CatalystBitwise[Int] = of[Int]
  implicit val framelesslongBitwise: CatalystBitwise[Long] = of[Long]
}
