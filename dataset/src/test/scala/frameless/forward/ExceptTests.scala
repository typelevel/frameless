package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ExceptTests extends TypedDatasetSuite {
  test("except") {
    def prop[A: TypedEncoder](data1: Set[A], data2: Set[A]): Prop = {
      val dataset1 = TypedDataset.create(data1.toSeq)
      val dataset2 = TypedDataset.create(data2.toSeq)
      val datasetSubtract = dataset1.except(dataset2).collect().run().toVector
      val dataSubtract = data1.diff(data2)

      Prop.all(
        datasetSubtract.size ?= dataSubtract.size,
        datasetSubtract.toSet ?= dataSubtract
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
