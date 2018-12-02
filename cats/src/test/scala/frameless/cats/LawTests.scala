package frameless.cats


import cats.laws.discipline.MonadTests
import cats.tests.CatsSuite
import frameless.Job
import org.apache.spark.sql.SparkSession
import jobber._
import org.scalacheck.{Arbitrary, Gen}

class LawTests extends CatsSuite with SparkTests {
  implicit val spark: SparkSession = session
   val  genJob: Gen[Job[Int]] = for {
    i: Int <- Arbitrary.arbitrary[Int]
  }yield Job(i)

  val  genJob2: Gen[Job[Function1[Int,Int]]] = for {
    a: Int <- Arbitrary.arbitrary[Int]
  }yield Job((b: Int) => a + b)

  implicit val arbJob: Arbitrary[Job[Int]] = Arbitrary(genJob)
  implicit val arbJob2: Arbitrary[Job[Function1[Int,Int]]] = Arbitrary(genJob2)

  checkAll("Job.MonadLaws", MonadTests[Job].monad[Int,Int,Int])
}