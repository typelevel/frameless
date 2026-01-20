package frameless.ml.feature

import frameless.ml.FramelessMlSuite
import frameless.ml.feature.TypedOneHotEncoder.HandleInvalid

import org.apache.spark.ml.linalg._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import shapeless.test.illTyped

final class TypedOneHotEncoderTests extends FramelessMlSuite {

  test(".fit() returns a correct TypedTransformer") {
    implicit val arbInt = Arbitrary(Gen.choose(0, 99))
    def prop[A: TypedEncoder : Arbitrary] = forAll { (x2: X2[Int, A], dropLast: Boolean) =>
      val encoder = TypedOneHotEncoder[X1[Int]].setDropLast(dropLast)
      val inputs = 0.to(x2.a).map(i => X2(i, x2.b))
      val ds = TypedDataset.create(inputs)
      val model = encoder.fit(ds).run()
      val resultDs = model.transform(TypedDataset.create(Seq(x2))).as[X3[Int, A, Vector]]
      val result = resultDs.collect.run()
      if (dropLast) {
        result == Seq (X3(x2.a, x2.b,
          Vectors.sparse(x2.a, Array.emptyIntArray, Array.emptyDoubleArray)))
      } else {
        result == Seq (X3(x2.a, x2.b,
          Vectors.sparse(x2.a + 1, Array(x2.a), Array(1.0))))
      }
    }

    check(prop[Double])
    check(prop[String])
  }

  test("param setting is retained") {
    implicit val arbHandleInvalid: Arbitrary[HandleInvalid] = Arbitrary {
      Gen.oneOf(HandleInvalid.Keep, HandleInvalid.Error)
    }

    val prop = forAll { handleInvalid: HandleInvalid =>
      val encoder = TypedOneHotEncoder[X1[Int]]
        .setHandleInvalid(handleInvalid)
      val ds = TypedDataset.create(Seq(X1(1)))
      val model = encoder.fit(ds).run()

      model.transformer.getHandleInvalid == handleInvalid.sparkValue
    }

    check(prop)
  }

  test("apply() compiles only with correct inputs") {
    illTyped("TypedOneHotEncoder.apply[Double]()")
    illTyped("TypedOneHotEncoder.apply[X1[Double]]()")
    illTyped("TypedOneHotEncoder.apply[X2[String, Long]]()")
  }
}
