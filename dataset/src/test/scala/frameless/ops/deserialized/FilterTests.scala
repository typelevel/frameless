package frameless
package ops
package deserialized

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FilterTests extends TypedDatasetSuite {
  test("filter") {
    def prop[A: TypedEncoder](filterFunction: A => Boolean, data: Vector[A]): Prop =
      TypedDataset.create(data).
        deserialized.
        filter(filterFunction).
        collect().run().toVector =? data.filter(filterFunction)

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
