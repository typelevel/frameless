package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class CastTests extends TypedDatasetSuite {

  def prop[A: TypedEncoder, B: TypedEncoder](a: A)(
    implicit
    view: A => B,
    cast: CatalystCast[A, B]
  ): Prop = {
    val df = TypedDataset.create(X1(a) :: Nil)
    val got = df.select(df.col('a).cast[B]).collect().run()

    got ?= (view(a) :: Nil)
  }

  test("cast") {
    check(prop[Int, Double] _)
  }

}
