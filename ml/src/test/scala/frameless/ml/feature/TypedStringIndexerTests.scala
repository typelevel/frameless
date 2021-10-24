package frameless
package ml
package feature

import frameless.ml.feature.TypedStringIndexer.HandleInvalid
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import shapeless.test.illTyped
import org.scalatest.matchers.must.Matchers

class TypedStringIndexerTests extends FramelessMlSuite with Matchers {

  test(".fit() returns a correct TypedTransformer") {
    def prop[A: TypedEncoder : Arbitrary] = forAll { x2: X2[String, A] =>
      val indexer = TypedStringIndexer[X1[String]]
      val ds = TypedDataset.create(Seq(x2))
      val model = indexer.fit(ds).run()
      val resultDs = model.transform(ds).as[X3[String, A, Double]]()

      resultDs.collect().run() == Seq(X3(x2.a, x2.b, 0D))
    }

    check(prop[Double])
    check(prop[String])
  }

  test("param setting is retained") {
    implicit val arbHandleInvalid: Arbitrary[HandleInvalid] = Arbitrary {
      Gen.oneOf(HandleInvalid.Keep, HandleInvalid.Error, HandleInvalid.Skip)
    }

    val prop = forAll { handleInvalid: HandleInvalid =>
      val indexer = TypedStringIndexer[X1[String]]
        .setHandleInvalid(handleInvalid)
      val ds = TypedDataset.create(Seq(X1("foo")))
      val model = indexer.fit(ds).run()

      model.transformer.getHandleInvalid == handleInvalid.sparkValue
    }

    check(prop)
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedStringIndexer.create[Double]()")
    illTyped("TypedStringIndexer.create[X1[Double]]()")
    illTyped("TypedStringIndexer.create[X2[String, Long]]()")
  }

}
