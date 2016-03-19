package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FirstTests extends TypedDatasetSuite {
  test("first") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop =
      TypedDataset.create(data).firstOption().run() =? data.headOption

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
