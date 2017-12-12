package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop.{forAll, _}

class SQLContextTests extends TypedDatasetSuite {
  test("sqlContext") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create[A](data)

      dataset.sqlContext.run() =? dataset.dataset.sqlContext
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
