package frameless
package ml
package feature

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.MustMatchers
import org.scalacheck.Prop._
import shapeless.test.illTyped

class TypedIndexToStringTests extends FramelessMlSuite with MustMatchers {

  test(".transform() correctly transform an input dataset") {
    implicit val arbDouble = Arbitrary(Gen.choose(1, 99).map(_.toDouble))

    def prop[A: TypedEncoder: Arbitrary] = forAll { x2: X2[Double, A] =>
      val transformer = TypedIndexToString.create[X1[Double]](Array.fill(99)("foo"))
      val ds = TypedDataset.create(Seq(x2))
      val ds2 = transformer.transform(ds)

      ds2.collect.run() == Seq((x2.a, x2.b, "foo"))
    }

    check(prop[Double])
    check(prop[String])
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedIndexToString.create[String](Array(\"foo\"))")
    illTyped("TypedIndexToString.create[X1[String]](Array(\"foo\"))")
    illTyped("TypedIndexToString.create[X1[Long]](Array(\"foo\"))")
    illTyped("TypedIndexToString.create[X2[String, Int]](Array(\"foo\"))")
  }

}
