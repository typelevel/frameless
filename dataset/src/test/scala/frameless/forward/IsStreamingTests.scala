package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class IsStreamingTests extends TypedDatasetSuite {
  test("isStreaming") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.isStreaming ?= dataset.dataset.isStreaming
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
