package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class SparkSessionTests extends TypedDatasetSuite {
  test("sparkSession") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create[A](data)

      dataset.sparkSession =? dataset.dataset.sparkSession
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}