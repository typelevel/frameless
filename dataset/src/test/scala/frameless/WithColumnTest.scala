package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class WithColumnTest extends TypedDatasetSuite {
  test("append five columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d = TypedDataset.create(X1(value) :: Nil)
      val d1 = d.withColumn(d('a))
      val d2 = d1.withColumn(d1('_1))
      val d3 = d2.withColumn(d2('_2))
      val d4 = d3.withColumn(d3('_3))
      val d5 = d4.withColumn(d4('_4))

      (value, value, value, value, value, value) ?= d5.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}
