package frameless
package ml
package clustering

import frameless.ml.classification.TypedKMeans
import frameless.{TypedDataset, TypedEncoder, X1, X2, X3}
import org.apache.spark.ml.linalg._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import frameless.ml._
import frameless.ml.params.kmeans.KMeansInitMode
import org.scalatest.matchers.must.Matchers

class KMeansTests extends FramelessMlSuite with Matchers {
  implicit val arbVector:  Arbitrary[Vector] =
    Arbitrary(Generators.arbVector.arbitrary)
  implicit val arbKMeansInitMode: Arbitrary[KMeansInitMode] =
    Arbitrary{
      Gen.oneOf(
        Gen.const(KMeansInitMode.KMeansPlusPlus),
        Gen.const(KMeansInitMode.Random)
      )
    }

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x1: X1[Vector] =>
      val km = TypedKMeans[X1[Vector]]
      val ds = TypedDataset.create(Seq(x1))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X2[Vector, Int]]()

      pDs.select(pDs.col('a)).collect().run().toList == Seq(x1.a)
    }

    def prop3[A: TypedEncoder : Arbitrary] = forAll { x2: X2[Vector, A] =>
      val km = TypedKMeans[X1[Vector]]
      val ds = TypedDataset.create(Seq(x2))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, A, Int]]()

      pDs.select(pDs.col('a), pDs.col('b)).collect().run() == Seq((x2.a, x2.b))
    }

    check(prop)
    check(prop3[Double])
  }

  test("param setting is retained") {
    val prop = forAll { initMode: KMeansInitMode =>
      val rf = TypedKMeans[X1[Vector]]
        .setInitMode(KMeansInitMode.Random)
        .setInitSteps(2)
        .setK(10)
        .setMaxIter(15)
        .setSeed(123223L)
        .setTol(12D)

      val ds = TypedDataset.create(Seq(X2(Vectors.dense(Array(0D)), 0)))
      val model = rf.fit(ds).run()

      model.transformer.getInitMode == KMeansInitMode.Random.sparkValue &&
        model.transformer.getInitSteps == 2 &&
        model.transformer.getK == 10 &&
        model.transformer.getMaxIter == 15 &&
        model.transformer.getSeed == 123223L &&
        model.transformer.getTol == 12D
    }

    check(prop)
  }
}
