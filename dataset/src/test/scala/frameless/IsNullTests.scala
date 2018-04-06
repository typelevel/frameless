package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers

class IsNullTests extends TypedDatasetSuite with Matchers{

  test("isNull and isNotNull") {
    def prop[A: TypedEncoder](a: Option[A]): Prop = {
      val df = TypedDataset.create(X1(a) :: Nil)
      val got = df.selectMany(df.col('a).isNull, df.col('a).isNotNull).collect().run()

      got ?= ((a.isEmpty , a.isDefined) :: Nil)
    }

    check(prop[BigDecimal] _)
    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
    check(prop[String] _)
  }
}
