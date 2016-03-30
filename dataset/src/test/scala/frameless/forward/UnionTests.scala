package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class UnionTests extends TypedDatasetSuite {
  test("Union") {
    def prop[A: TypedEncoder](data1: Vector[A], data2: Vector[A]): Prop = {
      val dataset1 = TypedDataset.create(data1)
      val dataset2 = TypedDataset.create(data2)
      val datasetUnion = dataset1.union(dataset2).collect().run().toVector
      val dataUnion  = data1.union(data2)

      datasetUnion ?= dataUnion
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
