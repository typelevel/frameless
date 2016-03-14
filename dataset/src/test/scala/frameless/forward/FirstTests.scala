package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FirstTests extends TypedDatasetSuite {
  test("first") {
    def prop[A](data: Vector[A])(implicit e: TypedEncoder[A]): Prop =
      TypedDataset.create(data).firstOption().run() =? data.headOption

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
