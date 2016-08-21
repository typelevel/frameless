package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class LimitTests extends TypedDatasetSuite {
  test("limit") {
    def prop[A: TypedEncoder](data: Vector[A], n: Int): Prop = (n >= 0) ==> {
      val dataset = TypedDataset.create(data).limit(n).collect().run()

      Prop.all(
        dataset.length ?= Math.min(data.length, n),
        dataset.forall(data.contains)
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
