package frameless.ml.clustering

import frameless.ml.classification.TypedKMeans
import frameless.{TypedDataset, TypedEncoder, X1, X2, X3}
import org.apache.spark.ml.linalg._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.MustMatchers
import frameless.ml._


class KMeansTests extends FramelessMlSuite with MustMatchers {
  implicit val arbVectorNonEmpty: Arbitrary[Vector] =
    Arbitrary(Generators.arbVector.arbitrary suchThat (_.size > 0))

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x1: X1[Vector] =>
      val km = TypedKMeans[X1[Vector]](2)
      val ds = TypedDataset.create(Seq(x1))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X2[Vector, Int]]

      pDs.select(pDs.col('a)).collect().run().toList == Seq(x1.a)

    }

    def prop3[A: TypedEncoder : Arbitrary] = forAll { x2: X2[Vector, A] =>
      val km = TypedKMeans[X1[Vector]](2)
      val ds = TypedDataset.create(Seq(x2))
      val model = km.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, A, Int]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq((x2.a, x2.b))
    }

    check(prop)
    check(prop3[Double])
  }

  test("param setting is retained") {
    case class Data(featuresCol: Vector, predictionCol: Int)
    case class Input(featuresCol: Vector)

    val rf = TypedKMeans[Input]()
      .setFeaturesCol("featuresCol")
      .setPredictionCol("predictionCol")
      .setInitMode("random")
      .setInitSteps(2)
      .setK(10)
      .setMaxIter(15)
      .setSeed(123223L)
      .setTol(12D)

    val ds = TypedDataset.create(Seq(Data(Vectors.dense(Array(0D)),0)))
    val model = rf.fit(ds).run()

    model.transformer.getInitMode == "random" &&
      model.transformer.getInitSteps == 2 &&
      model.transformer.getK == 10 &&
      model.transformer.getMaxIter == 15 &&
      model.transformer.getSeed == 123223L &&
      model.transformer.getTol == 12D

  }
}
