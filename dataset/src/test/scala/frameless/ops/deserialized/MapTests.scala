package frameless
package ops
package deserialized

import org.scalacheck.Prop
import org.scalacheck.Prop._

class MapTests extends TypedDatasetSuite {
  test("map") {
    def prop[A: TypedEncoder, B: TypedEncoder](mapFunction: A => B, data: Vector[A]): Prop =
      TypedDataset.create(data).deserialized.map(mapFunction).collect().run().toVector =? data
        .map(mapFunction)

    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, Int] _))
    check(forAll(prop[X1[Int], X1[Int]] _))
  }
}
