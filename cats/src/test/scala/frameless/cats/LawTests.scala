package frameless.cats


import cats.laws.discipline.{FunctorTests, MonadTests}
import cats.tests.CatsSuite
import frameless.{Job, TypedDatasetSuite}
import org.scalacheck.ScalacheckShapeless._
import jobber._
import org.scalacheck.{Arbitrary, Gen}



class LawTests extends  TypedDatasetSuite  with CatsSuite {


  implicit def genJob[A: Arbitrary]: Gen[Job[A]] = for {
   a <- Arbitrary.arbitrary[A]
  } yield Job(a)

  implicit def arbJob[A: Arbitrary]: Arbitrary[Job[A]] = Arbitrary[Job[A]](genJob)
    checkAll("Job.MonadLaws", MonadTests[Job].monad[Int,String,Map[String,Int]])
    checkAll("Job.FunctorLaws", FunctorTests[Job].functor[Int,String,Map[String,Int]])
}