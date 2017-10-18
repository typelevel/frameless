package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers

class FirstTests extends TypedDatasetSuite with Matchers {
  test("first") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop =
      TypedDataset.create(data).firstOption().run() =? data.headOption

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("first on empty dataset should return None") {
    TypedDataset.create(Vector[Int]()).firstOption().run() shouldBe None
  }
}
