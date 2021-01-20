package frameless

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConverters._
import org.scalatest.matchers.should.Matchers

class RandomSplitTests extends TypedDatasetSuite with Matchers {

  val nonEmptyPositiveArray: Gen[Array[Double]] = Gen.nonEmptyListOf(Gen.posNum[Double]).map(_.toArray)

  test("randomSplit(weight, seed)") {
    def prop[A: TypedEncoder : Arbitrary] = forAll(vectorGen[A], nonEmptyPositiveArray, arbitrary[Long]) {
      (data: Vector[A], weights: Array[Double], seed: Long) =>
        val dataset = TypedDataset.create(data)

        dataset.randomSplit(weights, seed).map(_.count().run()) sameElements
          dataset.dataset.randomSplit(weights, seed).map(_.count())
    }

    check(prop[Int])
    check(prop[String])
  }

  test("randomSplitAsList(weight, seed)") {
    def prop[A: TypedEncoder : Arbitrary] = forAll(vectorGen[A], nonEmptyPositiveArray, arbitrary[Long]) {
      (data: Vector[A], weights: Array[Double], seed: Long) =>
        val dataset = TypedDataset.create(data)

        dataset.randomSplitAsList(weights, seed).asScala.map(_.count().run()) sameElements
          dataset.dataset.randomSplitAsList(weights, seed).asScala.map(_.count())
    }

    check(prop[Int])
    check(prop[String])
  }
}
