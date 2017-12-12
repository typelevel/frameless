package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop.{forAll, _}

class QueryExecutionTests extends TypedDatasetSuite {
  test("queryExecution") {
    def prop[A: TypedEncoder](data: Vector[A]): Prop = {
      val dataset = TypedDataset.create[A](data)

      dataset.queryExecution =? dataset.dataset.queryExecution
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}