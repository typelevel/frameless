package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class TakeTests extends TypedDatasetSuite {
  test("take") {
    def prop[A](n: Int, data: Vector[A])(implicit e: TypedEncoder[A]): Prop =
      TypedDataset.create(data).take(n).run().toVector =? data.take(n)

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
