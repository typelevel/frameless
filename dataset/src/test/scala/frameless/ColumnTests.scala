package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.math.Ordering.Implicits._

class ColumnTests extends TypedDatasetSuite {

  test("select('a < 'b, 'a <= 'b, 'a > 'b, 'a >= 'b)") {
    def prop[A: TypedEncoder : frameless.CatalystOrdered : scala.math.Ordering](a: A, b: A): Prop = {
      val dataset = TypedDataset.create(X2(a, b) :: Nil)
      val A = dataset.col('a)
      val B = dataset.col('b)

      val dataset2 = dataset.selectMany(
        A < B, A < b,   // One test uses columns, other uses literals
        A <= B, A <= b,
        A > B, A > b,
        A >= B, A >= b
      ).collect().run().toVector

      dataset2 ?= Vector((a < b, a < b, a <= b, a <= b, a > b, a > b, a >= b, a >= b))

    }

    implicit val sqlDateOrdering: Ordering[SQLDate] = Ordering.by(_.days)
    implicit val sqlTimestmapOrdering: Ordering[SQLTimestamp] = Ordering.by(_.us)

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
  }
}
