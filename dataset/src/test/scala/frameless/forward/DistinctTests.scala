package frameless
package forward

import org.scalacheck.Prop
import org.scalacheck.Prop._
import math.Ordering

class DistinctTests extends TypedDatasetSuite {
  test("distinct") {
    // Comparison done with `.sorted` because order is not preserved by Spark for this operation.
    def prop[A](data: Vector[A])(implicit e: TypedEncoder[A], o: Ordering[A]): Prop =
      TypedDataset.create(data).distinct.collect().run().toVector.sorted ?= data.distinct.sorted

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
