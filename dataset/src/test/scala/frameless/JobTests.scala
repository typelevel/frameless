package frameless

import org.scalacheck.Arbitrary
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class JobTests
    extends AnyFreeSpec
    with BeforeAndAfterAll
    with SparkTesting
    with ScalaCheckDrivenPropertyChecks
    with Matchers {

  "map" - {
    "identity" in {
      def check[T](
          implicit
          arb: Arbitrary[T]
        ) = forAll { t: T =>
        Job(t).map(identity).run() shouldEqual Job(t).run()
      }

      check[Int]
    }

    val f1: Int => Int = _ + 1
    val f2: Int => Int = (i: Int) => i * i

    "composition" in forAll { i: Int =>
      Job(i).map(f1).map(f2).run() shouldEqual Job(i).map(f1 andThen f2).run()
    }
  }

  "flatMap" - {
    val f1: Int => Job[Int] = (i: Int) => Job(i + 1)
    val f2: Int => Job[Int] = (i: Int) => Job(i * i)

    "left identity" in forAll { i: Int =>
      Job(i).flatMap(f1).run() shouldEqual f1(i).run()
    }

    "right identity" in forAll { i: Int =>
      Job(i).flatMap(i => Job.apply(i)).run() shouldEqual Job(i).run()
    }

    "associativity" in forAll { i: Int =>
      Job(i).flatMap(f1).flatMap(f2).run() shouldEqual Job(i)
        .flatMap(ii => f1(ii).flatMap(f2))
        .run()
    }
  }

  "properties" - {
    "read back" in forAll { (k: String, v: String) =>
      val scopedKey = "frameless.tests." + k
      Job(1).withLocalProperty(scopedKey, v).run()
      sc.getLocalProperty(scopedKey) shouldBe v
    }
  }
}
