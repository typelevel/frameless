package frameless
package ml
package feature

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.MustMatchers
import shapeless.test.illTyped

class TypedStringIndexerTests extends FramelessMlSuite with MustMatchers {

  test(".fit() returns a correct TypedTransformer") {
    def prop[A: TypedEncoder : Arbitrary] = forAll { x2: X2[String, A] =>
      val indexer = TypedStringIndexer.create[X1[String]]()
      val ds = TypedDataset.create(Seq(x2))
      val model = indexer.fit(ds)
      val resultDs = model.transform(ds).as[X3[String, A, Double]]

      resultDs.collect.run() == Seq(X3(x2.a, x2.b, 0D))
    }

    check(prop[Double])
    check(prop[String])
  }

  test("param setting is retained") {
    val indexer = TypedStringIndexer.create[X1[String]]()
      .setHandleInvalid(TypedStringIndexer.HandleInvalid.Keep)
    val ds = TypedDataset.create(Seq(X1("foo")))
    val model = indexer.fit(ds)

    model.transformer.getHandleInvalid mustEqual "keep"
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedStringIndexer.create[Double]()")
    illTyped("TypedStringIndexer.create[X1[Double]]()")
    illTyped("TypedStringIndexer.create[X2[String, Long]]()")
  }

}
