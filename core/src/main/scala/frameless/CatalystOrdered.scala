package frameless

import scala.annotation.implicitNotFound
import shapeless.{ Generic, HList, Lazy }
import shapeless.ops.hlist.LiftAll
import java.time.{ Duration, Instant, Period }

/** Types that can be ordered/compared by Catalyst. */
@implicitNotFound("Cannot compare columns of type ${A}.")
trait CatalystOrdered[A]

object CatalystOrdered {
  private[this] val theInstance = new CatalystOrdered[Any] {}

  private[this] def of[A]: CatalystOrdered[A] =
    theInstance.asInstanceOf[CatalystOrdered[A]]

  implicit val framelessIntOrdered: CatalystOrdered[Int] = of[Int]
  implicit val framelessBooleanOrdered: CatalystOrdered[Boolean] = of[Boolean]
  implicit val framelessByteOrdered: CatalystOrdered[Byte] = of[Byte]
  implicit val framelessShortOrdered: CatalystOrdered[Short] = of[Short]
  implicit val framelessLongOrdered: CatalystOrdered[Long] = of[Long]
  implicit val framelessFloatOrdered: CatalystOrdered[Float] = of[Float]
  implicit val framelessDoubleOrdered: CatalystOrdered[Double] = of[Double]

  implicit val framelessBigDecimalOrdered: CatalystOrdered[BigDecimal] =
    of[BigDecimal]
  implicit val framelessSQLDateOrdered: CatalystOrdered[SQLDate] = of[SQLDate]

  implicit val framelessSQLTimestampOrdered: CatalystOrdered[SQLTimestamp] =
    of[SQLTimestamp]
  implicit val framelessStringOrdered: CatalystOrdered[String] = of[String]
  implicit val framelessInstantOrdered: CatalystOrdered[Instant] = of[Instant]

  implicit val framelessDurationOrdered: CatalystOrdered[Duration] =
    of[Duration]
  implicit val framelessPeriodOrdered: CatalystOrdered[Period] = of[Period]

  implicit def injectionOrdered[A, B](
      implicit
      i0: Injection[A, B],
      i1: CatalystOrdered[B]
    ): CatalystOrdered[A] = of[A]

  implicit def deriveGeneric[G, H <: HList](
      implicit
      i0: Generic.Aux[G, H],
      i1: Lazy[LiftAll[CatalystOrdered, H]]
    ): CatalystOrdered[G] = of[G]
}
