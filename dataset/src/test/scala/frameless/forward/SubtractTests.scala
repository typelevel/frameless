package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import math.Ordering

class SubtractTests extends TypedDatasetSuite {
  test("subtract") {
    def prop[A](data1: Vector[A], data2: Vector[A])(implicit e: TypedEncoder[A], o: Ordering[A]): Prop = {
      val dataset1 = TypedDataset.create(data1)
      val dataset2 = TypedDataset.create(data2)
      val datasetSubtract = dataset1.subtract(dataset2).collect().run().toVector
      val dataSubtract = data1.filterNot(data2 contains _)

      // Comparison done with `.sorted` because order is not preserved by Spark for this operation.
      datasetSubtract.sorted ?= dataSubtract.sorted
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
