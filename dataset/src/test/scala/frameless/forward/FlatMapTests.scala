package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FlatMapTests extends TypedDatasetSuite {
  test("flatMap") {
    def prop[A, B](flatMapFunction: A => Vector[B], data: Vector[A])
      (implicit a: TypedEncoder[A], b: TypedEncoder[B]): Prop =
        TypedDataset.create(data).flatMap(flatMapFunction).collect().run().toVector =? data.flatMap(flatMapFunction)

    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, Int] _))
  }
}
