package frameless

import scala.annotation.implicitNotFound

/**
 * Spark does not return always Int on shift
 */

@implicitNotFound("Cannot do bit shift operations on columns of type ${In}.")
trait CatalystBitShift[In, Out]

object CatalystBitShift {
  private[this] val theInstance = new CatalystBitShift[Any, Any] {}
  private[this] def of[In, Out]: CatalystBitShift[In, Out] =
    theInstance.asInstanceOf[CatalystBitShift[In, Out]]

  implicit val framelessBitShiftBigDecimal: CatalystBitShift[BigDecimal, Int] =
    of[BigDecimal, Int]
  implicit val framelessBitShiftDouble: CatalystBitShift[Byte, Int] = of[Byte, Int]
  implicit val framelessBitShiftInt: CatalystBitShift[Short, Int] = of[Short, Int]
  implicit val framelessBitShiftLong: CatalystBitShift[Int, Int] = of[Int, Int]
  implicit val framelessBitShiftShort: CatalystBitShift[Long, Long] = of[Long, Long]
}
