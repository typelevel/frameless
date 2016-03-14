package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ReduceTests extends TypedDatasetSuite {
  test("reduce") {
    def prop[A](reduceFunction: (A, A) => A, data: Vector[A])(implicit e: TypedEncoder[A]): Prop =
      TypedDataset.create(data).reduceOption(reduceFunction).run() =? data.reduceOption(reduceFunction)

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
