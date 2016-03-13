package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ReduceTests extends TypedDatasetSuite {
  test("reduce") {
    def prop[A](reduceFunction: (A, A) => A, data: Vector[A])(implicit e: TypedEncoder[A]): Prop = {
      val reduced = TypedDataset.create(data).reduce(reduceFunction)
      if (data.isEmpty)
        intercept[UnsupportedOperationException](reduced.run()).getMessage =? "empty collection"
      else
        reduced.run() =? data.reduce(reduceFunction)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
