package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class IsLocalTests extends TypedDatasetSuite {
  test("isLocal") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.isLocal ?= dataset.dataset.isLocal
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
