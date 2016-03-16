package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FilterTests extends TypedDatasetSuite {
  test("filter") {
    def prop[A: TypedEncoder](elem: A, data: Vector[X1[A]])(implicit ex1: TypedEncoder[X1[A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)

      val dataset2 = dataset.filter(A === elem).collect().run().toVector
      val data2 = data.filter(_.a == elem)

      dataset2 ?= data2
    }

    check(forAll { (elem: Int, data: Vector[X1[Int]]) => prop(elem, data) })
  }
}
