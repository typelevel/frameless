package frameless
package ml
package regression

import frameless.ml.params.trees.FeatureSubsetStrategy
import shapeless.test.illTyped
import org.apache.spark.ml.linalg._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.MustMatchers

class TypedRandomForestRegressorTests extends FramelessMlSuite with MustMatchers {
  implicit val arbVectorNonEmpty: Arbitrary[Vector] =
    Arbitrary(Generators.arbVector.arbitrary suchThat (_.size > 0)) // vector must not be empty for RandomForestRegressor
  import Generators.arbTreesFeaturesSubsetStrategy

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x2: X2[Double, Vector] =>
      val rf = TypedRandomForestRegressor[X2[Double, Vector]]
      val ds = TypedDataset.create(Seq(x2))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Double, Vector, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }

    val prop2 = forAll { x2: X2[Vector, Double] =>
      val rf = TypedRandomForestRegressor[X2[Vector, Double]]
      val ds = TypedDataset.create(Seq(x2))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, Double, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }

    def prop3[A: TypedEncoder: Arbitrary] = forAll { x3: X3[Vector, Double, A] =>
      val rf = TypedRandomForestRegressor[X2[Vector, Double]]
      val ds = TypedDataset.create(Seq(x3))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).as[X4[Vector, Double, A, Double]]

      pDs.select(pDs.col('a), pDs.col('b), pDs.col('c)).collect.run() == Seq((x3.a, x3.b, x3.c))
    }

    check(prop)
    check(prop2)
    check(prop3[String])
    check(prop3[Double])
  }

  test("param setting is retained") {
    val prop = forAll { featureSubsetStrategy: FeatureSubsetStrategy =>
      val rf = TypedRandomForestRegressor[X2[Double, Vector]]
        .setNumTrees(10)
        .setMaxBins(100)
        .setFeatureSubsetStrategy(featureSubsetStrategy)
        .setMaxDepth(10)
        .setMaxMemoryInMB(100)
        .setMinInfoGain(0.1D)
        .setMinInstancesPerNode(2)
        .setSubsamplingRate(0.9D)

      val ds = TypedDataset.create(Seq(X2(0D, Vectors.dense(0D))))
      val model = rf.fit(ds).run()

      model.transformer.getNumTrees == 10 &&
        model.transformer.getMaxBins == 100 &&
        model.transformer.getFeatureSubsetStrategy == featureSubsetStrategy.sparkValue &&
        model.transformer.getMaxDepth == 10 &&
        model.transformer.getMaxMemoryInMB == 100 &&
        model.transformer.getMinInfoGain == 0.1D &&
        model.transformer.getMinInstancesPerNode == 2 &&
        model.transformer.getSubsamplingRate == 0.9D
    }

    check(prop)
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedRandomForestRegressor.create[Double]()")
    illTyped("TypedRandomForestRegressor.create[X1[Double]]()")
    illTyped("TypedRandomForestRegressor.create[X2[Double, Double]]()")
    illTyped("TypedRandomForestRegressor.create[X3[Vector, Double, Int]]()")
    illTyped("TypedRandomForestRegressor.create[X2[Vector, String]]()")
  }

}
