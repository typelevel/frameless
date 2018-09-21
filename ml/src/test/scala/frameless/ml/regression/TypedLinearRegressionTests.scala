package frameless
package ml
package regression

import frameless.ml.params.linears.{LossStrategy, Solver}
import org.apache.spark.ml.linalg._
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.Matchers._
import org.scalatest.{MustMatchers}
import shapeless.test.illTyped

class TypedLinearRegressionTests extends FramelessMlSuite with MustMatchers {

  implicit val arbVectorNonEmpty: Arbitrary[Vector] = Arbitrary(Generators.arbVector.arbitrary)

  test("fit() returns a correct TypedTransformer") {
    val prop = forAll { x2: X2[Double, Vector] =>
      val lr = TypedLinearRegression[X2[Double, Vector]]
      val ds = TypedDataset.create(Seq(x2))

      val model = lr.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Double, Vector, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }
    val prop2 = forAll { x2: X2[Vector, Double] =>
      val lr = TypedLinearRegression[X2[Vector, Double]]
      val ds = TypedDataset.create(Seq(x2))
      val model = lr.fit(ds).run()
      val pDs = model.transform(ds).as[X3[Vector, Double, Double]]

      pDs.select(pDs.col('a), pDs.col('b)).collect.run() == Seq(x2.a -> x2.b)
    }

    def prop3[A: TypedEncoder: Arbitrary] = forAll { x3: X3[Vector, Double, A] =>
      val lr = TypedLinearRegression[X2[Vector, Double]]
      val ds = TypedDataset.create(Seq(x3))
      val model = lr.fit(ds).run()
      val pDs = model.transform(ds).as[X4[Vector, Double, A, Double]]

      pDs.select(pDs.col('a), pDs.col('b), pDs.col('c)).collect.run() == Seq((x3.a, x3.b, x3.c))
    }

    check(prop)
    check(prop2)
    check(prop3[String])
    check(prop3[Double])
  }

  test("param setting is retained") {
    import Generators.{arbLossStrategy, arbSolver}

    val prop = forAll { (lossStrategy: LossStrategy, solver: Solver) =>
      val lr = TypedLinearRegression[X2[Double, Vector]]
        .setAggregationDepth(10)
        .setEpsilon(4)
        .setFitIntercept(true)
        .setLoss(lossStrategy)
        .setMaxIter(23)
        .setRegParam(1.2)
        .setStandardization(true)
        .setTol(2.3)
        .setSolver(solver)

      val ds = TypedDataset.create(Seq(X2(0D, Vectors.dense(0D))))
      val model = lr.fit(ds).run()

      model.transformer.getAggregationDepth == 10 &&
        model.transformer.getEpsilon == 4.0 &&
        model.transformer.getLoss == lossStrategy.sparkValue &&
        model.transformer.getMaxIter == 23 &&
        model.transformer.getRegParam == 1.2 &&
        model.transformer.getTol == 2.3 &&
        model.transformer.getSolver == solver.sparkValue
    }

    check(prop)
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedLinearRegressor.create[Double]()")
    illTyped("TypedLinearRegressor.create[X1[Double]]()")
    illTyped("TypedLinearRegressor.create[X2[Double, Double]]()")
    illTyped("TypedLinearRegressor.create[X3[Vector, Double, Int]]()")
    illTyped("TypedLinearRegressor.create[X2[Vector, String]]()")
  }

  test("TypedLinearRegressor should fit straight line ") {
    case class Point(features: Vector, labels: Double)

    val ds = Seq(
      X2(new DenseVector(Array(1.0)): Vector, 1.0),
      X2(new DenseVector(Array(2.0)): Vector, 2.0),
      X2(new DenseVector(Array(3.0)): Vector, 3.0),
      X2(new DenseVector(Array(4.0)): Vector, 4.0),
      X2(new DenseVector(Array(5.0)): Vector, 5.0),
      X2(new DenseVector(Array(6.0)): Vector, 6.0)
    )

    val ds2 = Seq(
      X3(new DenseVector(Array(1.0)): Vector,2F, 1.0),
      X3(new DenseVector(Array(2.0)): Vector,2F, 2.0),
      X3(new DenseVector(Array(3.0)): Vector,2F, 3.0),
      X3(new DenseVector(Array(4.0)): Vector,2F, 4.0),
      X3(new DenseVector(Array(5.0)): Vector,2F, 5.0),
      X3(new DenseVector(Array(6.0)): Vector,2F, 6.0)
    )

    val tds = TypedDataset.create(ds)

    val lr = TypedLinearRegression[X2[Vector, Double]]
      .setMaxIter(10)

    val model = lr.fit(tds).run()

    val tds2 = TypedDataset.create(ds2)

    val lr2 = TypedLinearRegression[X3[Vector, Float, Double]]
      .setMaxIter(10)

    val model2 = lr2.fit(tds2).run()

    model.transformer.coefficients shouldEqual new DenseVector(Array(1.0))
    model2.transformer.coefficients shouldEqual new DenseVector(Array(1.0))
  }
}
