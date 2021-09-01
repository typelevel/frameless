import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime => JavaLocalDateTime}

import org.scalacheck.{Arbitrary, Gen}

package object frameless {

  /**
   * Fixed decimal point to avoid precision problems specific to Spark
   */
  implicit val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary {
    for {
      x <- Gen.chooseNum(-1000, 1000)
      y <- Gen.chooseNum(0, 1000000)
    } yield BigDecimal(s"$x.$y")
  }

  /**
   * Fixed decimal point to avoid precision problems specific to Spark
   */
  implicit val arbDouble: Arbitrary[Double] = Arbitrary {
    arbBigDecimal.arbitrary.map(_.toDouble)
  }

  implicit val arbSqlDate = Arbitrary {
    Arbitrary.arbitrary[Int].map(SQLDate)
  }

  implicit val arbSqlTimestamp = Arbitrary {
    Arbitrary.arbitrary[Long].map(SQLTimestamp)
  }

  implicit def arbTuple1[A: Arbitrary] = Arbitrary {
    Arbitrary.arbitrary[A].map(Tuple1(_))
  }

  // see issue with scalacheck non serializable Vector: https://github.com/rickynils/scalacheck/issues/315
  implicit def arbVector[A](implicit A: Arbitrary[A]): Arbitrary[Vector[A]] =
    Arbitrary(Gen.listOf(A.arbitrary).map(_.toVector))

  def vectorGen[A: Arbitrary]: Gen[Vector[A]] = arbVector[A].arbitrary

  implicit val arbUdtEncodedClass: Arbitrary[UdtEncodedClass] = Arbitrary {
    for {
      int <- Arbitrary.arbitrary[Int]
      doubles <- Gen.listOf(arbDouble.arbitrary)
    } yield new UdtEncodedClass(int, doubles.toArray)
  }

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  implicit val localDateArb: Arbitrary[JavaLocalDateTime] = Arbitrary {
    for {
      year <- Gen.chooseNum(1900, 2027)
      month <- Gen.chooseNum(1, 12)
      dayOfMonth <- Gen.chooseNum(1, 28)
      hour <- Gen.chooseNum(1, 23)
      minute <- Gen.chooseNum(1, 59)
    } yield JavaLocalDateTime.of(year, month, dayOfMonth, hour, minute)
  }

  /**
   * LocalDateTime String Generator to test time related Spark functions
   */
  val dateTimeStringGen: Gen[List[String]] =
    for {
      listOfDates <- Gen.listOf(localDateArb.arbitrary)
      localDate <- listOfDates
    } yield localDate.format(dateTimeFormatter)

  val TEST_OUTPUT_DIR = "target/test-output"
}
