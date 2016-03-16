package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class CountTests extends TypedDatasetSuite {
  test("count") {
    def prop[A](data: Vector[A])(implicit e: TypedEncoder[A]): Prop =
      TypedDataset.create(data).count().run() ?= data.size.toLong

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
