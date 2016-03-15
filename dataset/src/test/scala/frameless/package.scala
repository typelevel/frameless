import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Test.Parameters
import org.scalatest.prop.Configuration.PropertyCheckConfig

package object frameless {
  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbBigDecimal = Arbitrary {
    for {
      x <- Gen.chooseNum(-1000, 1000)
      y <- Gen.chooseNum(0, 1000000)
    } yield BigDecimal(s"$x.$y")
  }

  implicit val arbSqlDate = Arbitrary {
    Arbitrary.arbitrary[Int].map(SQLDate)
  }

  implicit val arbSqlTimestamp = Arbitrary {
    Arbitrary.arbitrary[Long].map(SQLTimestamp)
  }
}
