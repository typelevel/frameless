package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class WithColumnTupledTest extends TypedDatasetSuite {
  test("append five columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d = TypedDataset.create(X1(value) :: Nil)
      val d1 = d.withColumnTupled(d('a))
      val d2 = d1.withColumnTupled(d1('_1))
      val d3 = d2.withColumnTupled(d2('_2))
      val d4 = d3.withColumnTupled(d3('_3))
      val d5 = d4.withColumnTupled(d4('_4))

      (value, value, value, value, value, value) ?= d5.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}
