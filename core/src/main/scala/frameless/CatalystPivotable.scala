package frameless

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot pivot on type ${A}. Currently supported types to pivot are {Int, Long, Boolean, and String}.")
trait CatalystPivotable[A]

object CatalystPivotable {
  def apply[A](implicit p: CatalystPivotable[A]): CatalystPivotable[A] = p

  implicit object IntPivotable extends CatalystPivotable[Int]
  implicit object LongPivotable extends CatalystPivotable[Long]
  implicit object BooleanPivotable extends CatalystPivotable[Boolean]
  implicit object StringPivotable extends CatalystPivotable[String]
}