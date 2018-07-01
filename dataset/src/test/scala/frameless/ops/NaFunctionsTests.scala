package frameless
package ops

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers._

class NaFunctionsTests extends TypedDatasetSuite {

  test("should be able to use `dropMany` with existing columns") {
    def prop[A <: Option[_] : TypedEncoder, B <: Option[_] : TypedEncoder]
    (data: List[X2[A, B]]): Prop = {

      val ds = TypedDataset.create(data)

      val dropped = ds.na.dropMany('a, 'b).collect().run()
      val expected = ds.filter(ds('a).isNotNone && ds('b).isNotNone).collect().run()

      dropped ?= expected
    }

    check(forAll(prop[Option[Int], Option[String]] _))
  }

  test("dropMany with an un-existing column should not compile") {
    val data = Seq(X1(0))
    val ds = TypedDataset.create(data)

    "ds.na.dropMany('b)" shouldNot typeCheck
  }
}