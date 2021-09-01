package frameless

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cannot find evidence that type ${A} is nullable. Currently, only Option[A] is nullable.")
trait CatalystNullable[A]

object CatalystNullable {
  implicit def optionIsNullable[A]: CatalystNullable[Option[A]] =
    new CatalystNullable[Option[A]] {}
}

@implicitNotFound("Cannot find evidence that type ${A} is not nullable.")
trait NotCatalystNullable[A]

object NotCatalystNullable {
  implicit def everythingIsNotNullable[A]: NotCatalystNullable[A] =
    new NotCatalystNullable[A] {}
  implicit def nullableIsNotNotNullable[A: CatalystNullable]: NotCatalystNullable[A] =
    new NotCatalystNullable[A] {}
}
