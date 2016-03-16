package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ForwardedFilterTests extends TypedDatasetSuite {
  test("filter") {
    def prop[A](filterFunction: A => Boolean, data: Vector[A])(implicit e: TypedEncoder[A]): Prop =
      TypedDataset.create(data).filter(filterFunction).collect().run().toVector =? data.filter(filterFunction)

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
