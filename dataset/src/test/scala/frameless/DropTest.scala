package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class DropTest extends TypedDatasetSuite {
  test("drop five columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d5 = TypedDataset.create(X5(value, value, value, value, value) :: Nil)
      val d4 = d5.drop('a)
      val d3 = d4.drop('_4)
      val d2 = d3.drop('_3)
      val d1 = d2.drop('_2)

      Tuple1(value) ?= d1.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}
