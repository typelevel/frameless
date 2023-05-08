import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime => JavaLocalDateTime}

import org.scalacheck.{Arbitrary, Gen}

package object frameless {
  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary {
    for {
      x <- Gen.chooseNum(-1000, 1000)
      y <- Gen.chooseNum(0, 1000000)
    } yield BigDecimal(s"$x.$y")
  }

  /** Fixed decimal point to avoid precision problems specific to Spark */
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

  /** LocalDateTime String Generator to test time related Spark functions */
  val dateTimeStringGen: Gen[List[String]] =
    for {
      listOfDates <- Gen.listOf(localDateArb.arbitrary)
      localDate <- listOfDates
    } yield localDate.format(dateTimeFormatter)

  val TEST_OUTPUT_DIR = "target/test-output"

  /**
   * Will dive down causes until either the cause is true or there are no more causes
   * @param t
   * @param f
   * @return
   */
  def anyCauseHas(t: Throwable, f: Throwable => Boolean): Boolean =
    if (f(t))
      true
    else
      if (t.getCause ne null)
        anyCauseHas(t.getCause, f)
      else
        false

  /**
   * Runs up to maxRuns and outputs the number of failures (times thrown)
   * @param maxRuns
   * @param thunk
   * @tparam T
   * @return the last passing thunk, or null
   */
  def runLoads[T](maxRuns: Int = 1000)(thunk: => T): T ={
    var i = 0
    var r = null.asInstanceOf[T]
    var passed = 0
    while(i < maxRuns){
      i += 1
      try {
        r = thunk
        passed += 1
        if (i % 20 == 0) {
          println(s"run $i successful")
        }
      } catch {
        case t: Throwable => System.err.println(s"failed unexpectedly on run $i - ${t.getMessage}")
      }
    }
    if (passed != maxRuns) {
      System.err.println(s"had ${maxRuns - passed} failures out of $maxRuns runs")
    }
    r
  }

    /**
   * Runs a given thunk up to maxRuns times, restarting the thunk if tolerantOf the thrown Throwable is true
   * @param tolerantOf
   * @param maxRuns default of 20
   * @param thunk
   * @return either a successful run result or the last error will be thrown
   */
  def tolerantRun[T](tolerantOf: Throwable => Boolean, maxRuns: Int = 20)(thunk: => T): T ={
    var passed = false
    var i = 0
    var res: T = null.asInstanceOf[T]
    var thrown: Throwable = null

    while((i < maxRuns) && !passed) {
      try {
        i += 1
        res = thunk
        passed = true
      } catch {
        case t: Throwable if anyCauseHas(t, tolerantOf) =>
          // rinse and repeat
          thrown = t
        case t: Throwable =>
          throw t
      }
    }
    if (!passed) {
      System.err.println(s"Despite being tolerant each of the $maxRuns runs failed, re-throwing the last")
      throw thrown
    }
    res
  }
}
