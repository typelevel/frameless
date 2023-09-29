package frameless

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot pivot on type ${A}. Currently supported types to pivot are {Int, Long, Boolean, and String}.")
trait CatalystPivotable[A]

object CatalystPivotable {
  private[this] val theInstance = new CatalystPivotable[Any] {}
  private[this] def of[A]: CatalystPivotable[A] = theInstance.asInstanceOf[CatalystPivotable[A]]

  implicit val framelessIntPivotable    : CatalystPivotable[Int]     = of[Int]
  implicit val framelessLongPivotable   : CatalystPivotable[Long]    = of[Long]
  implicit val framelessBooleanPivotable: CatalystPivotable[Boolean] = of[Boolean]
  implicit val framelessStringPivotable : CatalystPivotable[String]  = of[String]
}
