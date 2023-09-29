package frameless

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot do collection operations on columns of type ${C}.")
trait CatalystCollection[C[_]]

object CatalystCollection {
  private[this] val theInstance = new CatalystCollection[Any] {}
  private[this] def of[A[_]]: CatalystCollection[A] = theInstance.asInstanceOf[CatalystCollection[A]]

  implicit val arrayObject : CatalystCollection[Array]  = of[Array]
  implicit val seqObject   : CatalystCollection[Seq]    = of[Seq]
  implicit val listObject  : CatalystCollection[List]   = of[List]
  implicit val vectorObject: CatalystCollection[Vector] = of[Vector]
}
