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

  /**
   * copies a vector as we need two rows of the right dimension for 3.4's alg
   */
  def copyVector(vect: Vector): Vector = {
    val dubs = vect.toArray.map(_ % 2) // k is two
    val dense = Vectors.dense(dubs)
    vect match {
      case _: SparseVector => dense.toSparse
      case _ => dense
    }
  }

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x1: X1[Vector] =>
      val x1a = X1(copyVector(x1.a))
      val km = TypedKMeans[X1[Vector]]
      val ds = TypedDataset.create(Seq(x1, x1a))

      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X2[Vector, Int]]()

      pDs.select(pDs.col('a)).collect().run().toList == Seq(x1.a, x1a.a)
    }

    def prop3[A: TypedEncoder : Arbitrary] = forAll { x2: X2[Vector, A] =>
      val x2a = x2.copy(a = copyVector(x2.a))
      val km = TypedKMeans[X1[Vector]]
      val ds = TypedDataset.create(Seq(x2, x2a))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, A, Int]]()

      pDs.select(pDs.col('a), pDs.col('b)).collect().run().toList == Seq((x2.a, x2.b),(x2a.a, x2a.b))
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
