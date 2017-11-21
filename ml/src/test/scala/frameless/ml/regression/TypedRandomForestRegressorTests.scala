package frameless
package ml
package regression

import shapeless.test.illTyped
import org.apache.spark.ml.linalg._
import frameless.ml.regression.TypedRandomForestRegressor.FeatureSubsetStrategy
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.MustMatchers

class TypedRandomForestRegressorTests extends FramelessMlSuite with MustMatchers {
  implicit val arbVectorNonEmpty: Arbitrary[Vector] =
    Arbitrary(Generators.arbVector.arbitrary suchThat (_.size > 0)) // vector must not be empty for RandomForestRegressor

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x2: X2[Double, Vector] =>
      val rf = TypedRandomForestRegressor.create[X2[Double, Vector]]()
      val ds = TypedDataset.create(Seq(x2))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).run().as[X3[Double, Vector, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }

    val prop2 = forAll { x2: X2[Vector, Double] =>
      val rf = TypedRandomForestRegressor.create[X2[Vector, Double]]()
      val ds = TypedDataset.create(Seq(x2))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).run().as[X3[Vector, Double, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }

    def prop3[A: TypedEncoder: Arbitrary] = forAll { x3: X3[Vector, Double, A] =>
      val rf = TypedRandomForestRegressor.create[X2[Vector, Double]]()
      val ds = TypedDataset.create(Seq(x3))
      val model = rf.fit(ds).run()
      val pDs = model.transform(ds).run().as[X4[Vector, Double, A, Double]]

      pDs.select(pDs.col('a), pDs.col('b), pDs.col('c)).collect.run() == Seq((x3.a, x3.b, x3.c))
    }

    check(prop)
    check(prop2)
    check(prop3[String])
    check(prop3[Double])
  }

  test("param setting is retained") {
    val rf = TypedRandomForestRegressor.create[X2[Double, Vector]]()
      .setNumTrees(10)
      .setMaxBins(100)
      .setFeatureSubsetStrategy(FeatureSubsetStrategy.All)
      .setMaxDepth(10)

    val ds = TypedDataset.create(Seq(X2(0D, Vectors.dense(0D))))
    val model = rf.fit(ds).run()

    model.transformer.getNumTrees mustEqual 10
    model.transformer.getMaxBins mustEqual 100
    model.transformer.getFeatureSubsetStrategy mustEqual "all"
    model.transformer.getMaxDepth mustEqual 10
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedRandomForestRegressor.create[Double]()")
    illTyped("TypedRandomForestRegressor.create[X1[Double]]()")
    illTyped("TypedRandomForestRegressor.create[X2[Double, Double]]()")
    illTyped("TypedRandomForestRegressor.create[X3[Vector, Double, Int]]()")
    illTyped("TypedRandomForestRegressor.create[X2[Vector, String]]()")
  }

}
