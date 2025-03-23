package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ToJSONTests extends TypedDatasetSuite {
  test("toJSON") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.toJSON.collect().run() ?= dataset.dataset.toJSON.collect()
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
