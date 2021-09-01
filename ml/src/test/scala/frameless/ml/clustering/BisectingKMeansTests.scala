package frameless
package ml
package clustering

import frameless.{TypedDataset, TypedEncoder, X1, X2, X3}
import frameless.ml.classification.TypedBisectingKMeans
import org.scalacheck.Arbitrary
import org.apache.spark.ml.linalg._
import org.scalacheck.Prop._
import frameless.ml._
import org.scalatest.matchers.must.Matchers

class BisectingKMeansTests extends FramelessMlSuite with Matchers {
  implicit val arbVector: Arbitrary[Vector] =
    Arbitrary(Generators.arbVector.arbitrary)

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x1: X1[Vector] =>
      val km = TypedBisectingKMeans[X1[Vector]]()
      val ds = TypedDataset.create(Seq(x1))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X2[Vector, Int]]

      pDs.select(pDs.col('a)).collect().run().toList == Seq(x1.a)
    }

    def prop3[A: TypedEncoder: Arbitrary] = forAll { x2: X2[Vector, A] =>
      val km = TypedBisectingKMeans[X1[Vector]]
      val ds = TypedDataset.create(Seq(x2))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, A, Int]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq((x2.a, x2.b))
    }

    check(prop)
    check(prop3[Double])
  }

  test("param setting is retained") {
    val rf = TypedBisectingKMeans[X1[Vector]]()
      .setK(10)
      .setMaxIter(10)
      .setMinDivisibleClusterSize(1)
      .setSeed(123332)

    val ds = TypedDataset.create(Seq(X2(Vectors.dense(Array(0d)), 0)))
    val model = rf.fit(ds).run()

    model.transformer.getK == 10 &&
    model.transformer.getMaxIter == 10 &&
    model.transformer.getMinDivisibleClusterSize == 1 &&
    model.transformer.getSeed == 123332
  }
}
