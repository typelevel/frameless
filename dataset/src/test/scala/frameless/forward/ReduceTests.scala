package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ReduceTests extends TypedDatasetSuite {
  test("reduce") {
    def prop[A: TypedEncoder](reduceFunction: (A, A) => A, data: Vector[A]): Prop =
      TypedDataset.create(data).reduceOption(reduceFunction).run() =? data.reduceOption(reduceFunction)

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
