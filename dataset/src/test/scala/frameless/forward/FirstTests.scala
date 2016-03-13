package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FirstTests extends TypedDatasetSuite {
  test("first") {
    def prop[A](data: Vector[A])(implicit e: TypedEncoder[A]): Prop = {
      val first = TypedDataset.create(data).first()
      if (data.isEmpty)
        intercept[NoSuchElementException](first.run()).getMessage =? "next on empty iterator" // Nice one Spark
      else
        first.run() =? data.head
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
