package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ArithmeticTest extends TypedDatasetSuite {

  def prop[A: TypedEncoder: CatalystSummable: Numeric](a: A, b: A): Prop = {
    val df = TypedDataset.create(X2(a, b) :: Nil)
    val sum = implicitly[Numeric[A]].plus(a, b)
    val got = df.select(df.col('a) plus df.col('b)).collect().run()

    got ?= (sum :: Nil)
  }

  check(prop[Long] _)

}
